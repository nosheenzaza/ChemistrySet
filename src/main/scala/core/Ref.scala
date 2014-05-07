// Atomically updateable reference cells

package chemistry

import scala.annotation.tailrec
import java.util.concurrent.atomic._

/**
 * This is basically a wrapper of AtomicReference, of the most common policy
 * of what to do when a CAS fails. 
 */
final class Ref[A <: AnyRef](init: A) {
  /**
   * Pointer to atomic data, ensures atomicity if usage 
   * TODO look at the type specification to see which guarantees we have
   */
  private[chemistry] val data = new AtomicReference(init)
  
  /**
   * This is to doing something when a CAS to the atomic reference fails. That 
   * is, if there is a race to update the reference, we might ask another thread
   * to help out acheiving the operaton, in this case, no retry is needed.
   * 
   * TODO I cannot see what offering a read is baked in by default.
   */
  private[chemistry] val offers = new CircularPool[Offer[_]]

  /**
   * TODO learn what this does.
   */
  private[chemistry] def afterCAS {
    @tailrec def wakeFrom(n: offers.Node): Unit = if (n != null) {
      n.data.abortAndWake
      wakeFrom(n.next)
    }
    wakeFrom(offers.cursor)
  }  

  /**
   * 
   */
  private final case class Read[B](k: Reagent[A,B]) extends Reagent[Unit, B] {
    def tryReact(u: Unit, rx: Reaction, offer: Offer[B]): Any = {
      if (offer != null) offers.put(offer)
      data.get match {
	case null => Retry
	case ans => ans
      }
    }
    def composeI[C](next: Reagent[B,C]) = Read(k >> next)
    def maySync = k.maySync
    def alwaysCommits = k.alwaysCommits
    def snoop(u: Unit) = data.get match {
      case null => false
      case ans => k.snoop(ans)
    }
  }
  @inline def read: Reagent[Unit,A] = Read(Commit[A]())

  private final case class CAS[B](expect: A, update: A, k: Reagent[Unit,B]) 
		extends Reagent[Unit, B] {
    // CAS can ignore the "offer", because no information flows from the
    // ref cell to the continuation k.
    def tryReact(u: Unit, rx: Reaction, offer: Offer[B]): Any = 
      if (rx.canCASImmediate(k, offer)) {
	if (data.compareAndSet(expect, update))
	  k.tryReact((), rx, offer)
	else Retry
      } else k.tryReact((), rx.withCAS(Ref.this, expect, update), offer)

    def composeI[C](next: Reagent[B,C]) = CAS(expect, update, k >> next)
    def maySync = k.maySync
    def alwaysCommits = false
    def snoop(u: Unit) = false
  }
  @inline def cas(ov:A,nv:A): Reagent[Unit,Unit] = CAS(ov, nv, Commit[Unit]()) 

  /**
   * So this seems to be a reagent that takes care of updating the value of 
   * a ref. Why it is a reagent and takes another reagent, that I don't know.
   */
  abstract class InnerUpd[B,C,D] private[chemistry] (k: Reagent[C,D])
	   extends Reagent[B, D] {
    def tryReact(b: B, rx: Reaction, offer: Offer[D]): Any = {
      if (rx.canCASImmediate(k, offer)) {
	// no need to store offer here, as we will either succeed or retry
	// (never block)

	val ov = data.get
	if ((ov eq null) || !valid(ov,b)) return Retry
	val nv = newValue(ov, b)
	if (data.compareAndSet(ov, nv))
	  k.tryReact(retValue(ov, b), rx, offer)
	else Retry
      } else {
	if (offer != null) offers.put(offer) 

	val ov = data.get
	if ((ov eq null) || !valid(ov,b)) return Retry
	val nv = newValue(ov, b)
	k.tryReact(retValue(ov, b), rx.withCAS(Ref.this, ov, nv), offer)
      }
    }
    def composeI[E](next: Reagent[D,E]) = 
      new InnerUpd[B,C,E](k.compose(next)) {
	final def newValue(a: A, b: B): A = InnerUpd.this.newValue(a, b)
	final def retValue(a: A, b: B): C = InnerUpd.this.retValue(a, b)
      }
    def maySync = k.maySync
    def alwaysCommits = false
    def snoop(b: B) = false

    def valid(a: A, b: B): Boolean = true
    def newValue(a: A, b: B): A
    def retValue(a: A, b: B): C
    def retryValue(cur: A, lastAttempt: A, b: B): A = newValue(cur, b)
  }
  abstract class Upd[B,C] extends InnerUpd[B,C,C](Commit[C]()) // commit is a reagent itself, what it exactly does I don't know.

  /**
   * For the case of a Treiber Stack of Ints: return an InnerUpd reagent 
   * parameterized [Int, Unit, Unit] with a Commit reagent as a parameter
   * 
   * The mentioned type parameters are as follows:
   * A: the type of the ref Content I believe 
   * B: Input type to the update reagent
   * C return type of the update reagent
   * 
   * So for Triber stack as an instance, with function push, A is List[B], 
   * B is the parameter of List, and C is Unit.  
   */
  @inline def upd[B,C](f: (A,B) => (A,C)): Reagent[B, C] = 
    /* Upd is a subtype of reagent, here we provide implementations for the 
     * abstract methods 
     * B is the input type, and C is the return type */
    new Upd[B,C] {
      @inline def newValue(a: A, b: B): A = f(a,b)._1 
      @inline def retValue(a: A, b: B): C = f(a,b)._2 
    }

  @inline def upd[B](f: PartialFunction[A,(A,B)]): Reagent[Unit, B] =
    new Upd[Unit,B] {
      @inline override def valid(a: A, u: Unit): Boolean = f.isDefinedAt(a)
      @inline def newValue(a: A, u: Unit): A = f(a)._1
      @inline def retValue(a: A, u: Unit): B = f(a)._2
    }
 
}
object Ref {
  @inline def apply[A <: AnyRef](init: A): Ref[A] = new Ref(init)
  @inline def unapply[A <: AnyRef](r: Ref[A]): Option[A] = {
    while (true) r.data.get match {
      case null => {}
      case ans => return Some(ans)
    }
    throw Util.Impossible
  }
}

object upd {
  @inline def apply[A <: AnyRef,B,C](r: Ref[A])(f: (A,B) => (A,C)) = 
    r.upd(f)
  @inline def apply[A <: AnyRef,B](r: Ref[A])(f: PartialFunction[A, (A,B)]) = 
    r.upd(f)

/*
  @inline def fast[A <: AnyRef,B,C](r: Ref[A],f: (A,B) => (A,C)) = 
    new Reagent[B,C] {
      private val k = Commit[C]()
      def tryReact(b: B, rx: Reaction, offer: Offer[C]): Any = {
	if (rx.canCASImmediate(k, offer)) {
	  // no need to store offer here, as we will either succeed or retry
	  // (never block)

	  val ov = r.getI
	  val (nv, retVal) = f(ov, b)
	  if (r.casI(ov, nv)) retVal
	  else Retry
	} else {
	  if (offer != null) r.offers.put(offer) 

	  val ov = r.getI
	  val (nv, retVal) = f(ov, b)
	  k.tryReact(retVal, rx.withCAS(r, ov, nv), offer)
	}
      }
      def composeI[D](next: Reagent[C,D]) = throw Util.Impossible/*
	new r.InnerUpd[B,C,D](next) {
	  @inline def newValue(a: A, b: B): A = f(a,b)._1
	  @inline def retValue(a: A, b: B): C = f(a,b)._2
	}*/
      def maySync = false
      def alwaysCommits = false
      def snoop(b: B) = false
    }
*/
}
