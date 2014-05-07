package data

import java.util.Random
import sun.misc.Unsafe
import java.util.Comparator

import chemistry.Util._

/**
 * A simplified port of the concurrent skip list map of Doug Lea, with reagents.
 */
class ConcurrentSkipListMap[K, V] {

  /* ---------------- Nodes -------------- */

  /**
   * Nodes hold keys and values, and are singly linked in sorted
   * order, possibly with some intervening marker nodes. The list is
   * headed by a dummy node accessible as head.node. The value field
   * is declared only as Object because it takes special non-V
   * values for marker and header nodes.
   */
  final class Node[K, V](val key: Option[K], @volatile var value: Any, @volatile var next: Node[K, V]) {

    // UNSAFE mechanics
    /*ALL THESE MAY THROW EXCEPTIONS*/
    private val UNSAFE = sun.misc.Unsafe.getUnsafe()
    private val k = this.getClass();
    private val valueOffset: Long = UNSAFE.objectFieldOffset(k.getDeclaredField("value"));
    private val nextOffset: Long = UNSAFE.objectFieldOffset(k.getDeclaredField("next"));

    /**
     * Creates a new marker node. A marker is distinguished by
     * having its value field point to itself.  Marker nodes also
     * have null keys (here they are NONE), a fact that is exploited in a few places,
     * but this doesn't distinguish markers from the base-level
     * header node (head.node), which also has a null key.
     *
     * TODO be sure to handle the logic of none and null properly
     */
    def this(next: Node[K, V]) = {
      this(None, this, next)
    }

    /**
     * compareAndSet value field
     */
    def casValue(cmp: Any, v: Any) = UNSAFE.compareAndSwapObject(this, valueOffset, cmp, v)

    /**
     * compareAndSet next field
     */
    def casNext(cmp: Node[K, V], v: Node[K, V]) = UNSAFE.compareAndSwapObject(this, nextOffset, cmp, v)

    /**
     * Returns true if this node is a marker. This method isn't
     * actually called in any current code checking for markers
     * because callers will have already read value field and need
     * to use that read (not another done here) and so directly
     * test if value points to node.
     * @param n a possibly null reference to a node
     * @return true if this node is a marker node
     */
    def isMarker = value == this;

    /**
     * Returns true if this node is the header of base-level list.
     * @return true if this node is header node
     */
    def isBaseHeader = value == BASE_HEADER;

    /**
     * Tries to append a deletion marker to this node.
     * @param f the assumed current successor of this node
     * @return true if successful
     */
    def appendMarker(f: Node[K, V]) = casNext(f, new Node[K, V](f))

    /**
     * Helps out a deletion by appending marker or unlinking from
     * predecessor. This is called during traversals when value
     * field seen to be null.
     * @param b predecessor
     * @param f successor
     */
    def helpDelete(b: Node[K, V], f: Node[K, V]) {
      /*
             * Rechecking links and then doing only one of the
             * help-out stages per call tends to minimize CAS
             * interference among helping threads.
             */
      if (f == next && this == b.next) {
        if (f == null || f.value != f) // not already marked
          appendMarker(f);
        else
          b.casNext(this, f.next);
      }
    }

    /**
     * Returns value if this node contains a valid key-value pair,
     * else null.
     * @return this node's value if it isn't a marker or header or
     * is deleted, else null.
     */
    def getValidValue(): Option[V] = {
      val v: Any = value;
      if (v == this || v == BASE_HEADER) None
      Some(v.asInstanceOf[V]);
    }

    /**
     * Creates and returns a new SimpleImmutableEntry holding current
     * mapping if this node holds a valid value, else null.
     * @return new entry or null
     */
    //        AbstractMap.SimpleImmutableEntry[K,V] createSnapshot() {
    //            V v = getValidValue();
    //            if (v == null)
    //                return null;
    //            return new AbstractMap.SimpleImmutableEntry[K,V](key, v);
    //        }
  }

  /* ---------------- Indexing -------------- */

  /**
   * Index nodes represent the levels of the skip list.  Note that
   * even though both Nodes and Indexes have forward-pointing
   * fields, they have different types and are handled in different
   * ways, that can't nicely be captured by placing field in a
   * shared abstract class.
   * 
   * So it seems to me that node and down cannot both point to something at the 
   * same time, wicked!
   * TODO use an either type for Node and Index.
   */
  class Index[K, V](
    var node: Node[K, V],
    var down: Index[K, V],
    @volatile var right: Index[K, V]) {

    private val UNSAFE = sun.misc.Unsafe.getUnsafe()
    private val k = this.getClass();
    private val rightOffset: Long = UNSAFE.objectFieldOffset(k.getDeclaredField("right"));

    /**
     * compareAndSet right field
     */
    final def casRight(cmp: Index[K, V], v: Index[K, V]): Boolean =
      UNSAFE.compareAndSwapObject(this, rightOffset, cmp, v);

    /**
     * Returns true if the node this indexes has been deleted.
     * @return true if indexed node is known to be deleted
     * 
     * Note how null is used to indicate a deleted node. This has nothing to do
     * with the node itself having null as a key, or maybe it does, 
     * they both should be null, hmmm, maybe since there is concurrency, there
     * can be situations where deletion information is not propagated completely
     * at some instances of time, in which case they are not null together.
     */
    final def indexesDeletedNode = node.value == null;

    /**
     * Tries to CAS newSucc as successor.  To minimize races with
     * unlink that may lose this index node, if the node being
     * indexed is known to be deleted, it doesn't try to link in.
     * @param succ the expected current successor
     * @param newSucc the new successor
     * @return true if successful
     */
    final def link(succ: Index[K, V], newSucc: Index[K, V]) = {
      val n: Node[K, V] = node
      newSucc.right = succ;
      n.value != null && casRight(succ, newSucc);
    }

    /**
     * Tries to CAS right field to skip over apparent successor
     * succ.  Fails (forcing a retraversal by caller) if this node
     * is known to be deleted.
     * @param succ the expected current successor
     * @return true if successful
     */
    final def unlink(succ: Index[K, V]) = !indexesDeletedNode && casRight(succ, succ.right);
  }

  /* ---------------- Head nodes -------------- */

  /**
   * Nodes heading each level keep track of their level.
   */
  final class HeadIndex[K, V](node: Node[K, V],
    down: Index[K, V],
    right: Index[K, V],
    val level: Int,

    /**
     * The comparator used to maintain order in this map, or null
     * if using natural ordering.
     * @serial
     */
    val comparator: Comparator[K] = null)
    extends Index[K, V](node, down, right) {}

  
   
  // Unsafe mechanics
    val UNSAFE = sun.misc.Unsafe.getUnsafe();
    val k = this.getClass();
    val headOffset = UNSAFE.objectFieldOffset(k.getDeclaredField("head"))
  
  /**
   * Special value used to identify base-level header
   */
  object BASE_HEADER

  val seedGenerator: Random = new Random()

  /**
   * The comparator used to maintain order in this map, or null
   * if using natural ordering.
   * @serial
   */
  //      private final Comparator<? super K> comparator;

  /**
   * Seed for simple random number generator.  Not volatile since it
   * doesn't matter too much if different threads don't see updates.
   */
  @transient var randomSeed: Int = seedGenerator.nextInt() | 0x0100;

  /**
   * The topmost head index of the skiplist.
   */
  @volatile @transient var head: HeadIndex[K, V] = new HeadIndex[K, V](new Node[K, V](null, BASE_HEADER, null),
    null, null, 1);  
  
  /**
     * compareAndSet head node
     */
    private def casHead(cmp: HeadIndex[K,V], v: HeadIndex[K,V])  =
        UNSAFE.compareAndSwapObject(this, headOffset, cmp, v);
    
    
     /* ---------------- Traversal -------------- */

    /**
     * Returns a base-level node with key strictly less than given key,
     * or the base-level header if there is no such node.  Also
     * unlinks indexes to deleted nodes found along the way.  Callers
     * rely on this side-effect of clearing indices to deleted nodes.
     * @param key the key
     * @return a predecessor of key
     * 
     * @reagent
     * TODO debug me
     */
    private def findPredecessor(key: Comparable[K] ): Node[K,V] =  { //TODO handle variance of comparable
        if (key == null)
            throw new NullPointerException(); // don't postpone errors

	    /**
	     * @param q is the node at which we start going right at a certain level
	     */
	    def goRight(q: Index[K, V]): Node[K, V] = {

	      def traverseandCleanUpLevel(q: Index[K, V], r: Index[K, V]): Node[K, V] = {
	        val n: Node[K, V] = r.node;
	        val k: K = n.key.getOrElse(null.asInstanceOf[K]) //TODO make sure this cast works
	
	        if (n.value == null) {
	          if (!q.unlink(r)) null // restart
	          traverseandCleanUpLevel(q, q.right); // reread r
	        }
	
	        if (key.compareTo(k) > 0) {
	          traverseandCleanUpLevel(r, r.right);
	        } else n //TODO be sure this would work
	      }
	      
	      val r = q.right
	      
	      // when we are at the end of a level, go down, then either 
	      // return a data node, or go further to the right
	      if (r == null) { // meaning we are at the end of a level and we need to proceed downwards
	        val d: Index[K, V] = q.down;
	        if (d == null) q.node else goRight(d) //TODO be sure this should call goRight
	          } 
	      
	      // this is what we do when we can go further to the right
	      else {
	        val valueOfInnerLoop = traverseandCleanUpLevel(q, r)
	        if (valueOfInnerLoop == null)
	          goRight(q)
	        else valueOfInnerLoop
	      }
        }
	    
	    goRight(head)
    }
    
    /**
     * Returns node holding key or null if no such, clearing out any
     * deleted nodes seen along the way.  Repeatedly traverses at
     * base-level looking for key starting at predecessor returned
     * from findPredecessor, processing base-level deletions as
     * encountered. Some callers rely on this side-effect of clearing
     * deleted nodes.
     *
     * Restarts occur, at traversal step centered on node n, if:
     *
     *   (1) After reading n's next field, n is no longer assumed
     *       predecessor b's current successor, which means that
     *       we don't have a consistent 3-node snapshot and so cannot
     *       unlink any subsequent deleted nodes encountered.
     *
     *   (2) n's value field is null, indicating n is deleted, in
     *       which case we help out an ongoing structural deletion
     *       before retrying.  Even though there are cases where such
     *       unlinking doesn't require restart, they aren't sorted out
     *       here because doing so would not usually outweigh cost of
     *       restarting.
     *
     *   (3) n is a marker or n's predecessor's value field is null,
     *       indicating (among other possibilities) that
     *       findPredecessor returned a deleted node. We can't unlink
     *       the node because we don't know its predecessor, so rely
     *       on another call to findPredecessor to notice and return
     *       some earlier predecessor, which it will do. This check is
     *       only strictly needed at beginning of loop, (and the
     *       b.value check isn't strictly needed at all) but is done
     *       each iteration to help avoid contention with other
     *       threads by callers that will fail to be able to change
     *       links, and so will retry anyway.
     *
     * The traversal loops in doPut, doRemove, and findNear all
     * include the same three kinds of checks. And specialized
     * versions appear in findFirst, and findLast and their
     * variants. They can't easily share code because each uses the
     * reads of fields held in locals occurring in the orders they
     * were performed.
     *
     * @param key the key
     * @return node holding key, or null if no such
     * 
     * @reagent
     */
    private Node[K,V] findNode(Comparable<? super K> key) {
        for (;;) {
            Node[K,V] b = findPredecessor(key);
            Node[K,V] n = b.next;
            for (;;) {
                if (n == null)
                    return null;
                Node[K,V] f = n.next;
                if (n != b.next)                // inconsistent read
                    break;
                Object v = n.value;
                if (v == null) {                // n is deleted
                    n.helpDelete(b, f);
                    break;
                }
                if (v == n || b.value == null)  // b is deleted
                    break;
                int c = key.compareTo(n.key);
                if (c == 0)
                    return n;
                if (c < 0)
                    return null;
                b = n;
                n = f;
            }
        }
    }
//
    /**
     * Gets value for key using findNode.
     * @param okey the key
     * @return the value, or null if absent
     */
    private def doGet(key: Comparable[K]): V  = {
        /*
         * Loop needed here and elsewhere in case value field goes
         * null just as it is about to be returned, in which case we
         * lost a race with a deletion, so must retry.
         * 
         * @reagent
         */
        for (;;) {
            Node[K,V] n = findNode(key);
            if (n == null)
                return null;
            Object v = n.value;
            if (v != null)
                return (V)v;
        }
    }
//
//    /* ---------------- Insertion -------------- */
//
//    /**
//     * Main insertion method.  Adds element if not present, or
//     * replaces value if present and onlyIfAbsent is false.
//     * @param kkey the key
//     * @param value  the value that must be associated with key
//     * @param onlyIfAbsent if should not insert if already present
//     * @return the old value, or null if newly inserted
//     */
//    private V doPut(K kkey, V value, boolean onlyIfAbsent) {
//        Comparable<? super K> key = comparable(kkey);
//        for (;;) {
//            Node[K,V] b = findPredecessor(key);
//            Node[K,V] n = b.next;
//            for (;;) {
//                if (n != null) {
//                    Node[K,V] f = n.next;
//                    if (n != b.next)               // inconsistent read
//                        break;
//                    Object v = n.value;
//                    if (v == null) {               // n is deleted
//                        n.helpDelete(b, f);
//                        break;
//                    }
//                    if (v == n || b.value == null) // b is deleted
//                        break;
//                    int c = key.compareTo(n.key);
//                    if (c > 0) {
//                        b = n;
//                        n = f;
//                        continue;
//                    }
//                    if (c == 0) {
//                        if (onlyIfAbsent || n.casValue(v, value))
//                            return (V)v;
//                        else
//                            break; // restart if lost race to replace value
//                    }
//                    // else c < 0; fall through
//                }
//
//                Node[K,V] z = new Node[K,V](kkey, value, n);
//                if (!b.casNext(n, z))
//                    break;         // restart if lost race to append to b
//                int level = randomLevel();
//                if (level > 0)
//                    insertIndex(z, level);
//                return null;
//            }
//        }
//    }
//
//    /**
//     * Returns a random level for inserting a new node.
//     * Hardwired to k=1, p=0.5, max 31 (see above and
//     * Pugh's "Skip List Cookbook", sec 3.4).
//     *
//     * This uses the simplest of the generators described in George
//     * Marsaglia's "Xorshift RNGs" paper.  This is not a high-quality
//     * generator but is acceptable here.
//     */
//    private def randomLevel() = {
//        var x = randomSeed;
//        x = x ^ x << 13; 
//        x = x ^ x >>> 17;
//        randomSeed = x ^ x << 5;
//        if ((x & 0x80000001) != 0) 0 // test highest and lowest bits
//        
//        var level = 1;
//        while (((x = x >>> 1) & 1) != 0) level = 1 + level;
//        return level;
//    }
//
//    /**
//     * Creates and adds index nodes for the given node.
//     * @param z the node
//     * @param level the level of the index
//     */
//    private def insertIndex(Node[K,V] z, int level) { // no return
//        HeadIndex[K,V] h = head;
//        int max = h.level;
//
//        if (level <= max) {
//            Index[K,V] idx = null;
//            for (int i = 1; i <= level; ++i)
//                idx = new Index[K,V](z, idx, null);
//            addIndex(idx, h, level);
//
//        } else { // Add a new level
//            /*
//             * To reduce interference by other threads checking for
//             * empty levels in tryReduceLevel, new levels are added
//             * with initialized right pointers. Which in turn requires
//             * keeping levels in an array to access them while
//             * creating new head index nodes from the opposite
//             * direction.
//             */
//            level = max + 1;
//            Index[K,V][] idxs = (Index[K,V][])new Index[level+1];
//            Index[K,V] idx = null;
//            for (int i = 1; i <= level; ++i)
//                idxs[i] = idx = new Index[K,V](z, idx, null);
//
//            HeadIndex[K,V] oldh;
//            int k;
//            for (;;) {
//                oldh = head;
//                int oldLevel = oldh.level;
//                if (level <= oldLevel) { // lost race to add level
//                    k = level;
//                    break;
//                }
//                HeadIndex[K,V] newh = oldh;
//                Node[K,V] oldbase = oldh.node;
//                for (int j = oldLevel+1; j <= level; ++j)
//                    newh = new HeadIndex[K,V](oldbase, newh, idxs[j], j);
//                if (casHead(oldh, newh)) {
//                    k = oldLevel;
//                    break;
//                }
//            }
//            addIndex(idxs[k], oldh, k);
//        }
//    }
//
//    /**
//     * Adds given index nodes from given level down to 1.
//     * @param idx the topmost index node being inserted
//     * @param h the value of head to use to insert. This must be
//     * snapshotted by callers to provide correct insertion level
//     * @param indexLevel the level of the index
//     */
//    private void addIndex(Index[K,V] idx, HeadIndex[K,V] h, int indexLevel) {
//        // Track next level to insert in case of retries
//        int insertionLevel = indexLevel;
//        Comparable<? super K> key = comparable(idx.node.key);
//        if (key == null) throw new NullPointerException();
//
//        // Similar to findPredecessor, but adding index nodes along
//        // path to key.
//        for (;;) {
//            int j = h.level;
//            Index[K,V] q = h;
//            Index[K,V] r = q.right;
//            Index[K,V] t = idx;
//            for (;;) {
//                if (r != null) {
//                    Node[K,V] n = r.node;
//                    // compare before deletion check avoids needing recheck
//                    int c = key.compareTo(n.key);
//                    if (n.value == null) {
//                        if (!q.unlink(r))
//                            break;
//                        r = q.right;
//                        continue;
//                    }
//                    if (c > 0) {
//                        q = r;
//                        r = r.right;
//                        continue;
//                    }
//                }
//
//                if (j == insertionLevel) {
//                    // Don't insert index if node already deleted
//                    if (t.indexesDeletedNode()) {
//                        findNode(key); // cleans up
//                        return;
//                    }
//                    if (!q.link(r, t))
//                        break; // restart
//                    if (--insertionLevel == 0) {
//                        // need final deletion check before return
//                        if (t.indexesDeletedNode())
//                            findNode(key);
//                        return;
//                    }
//                }
//
//                if (--j >= insertionLevel && j < indexLevel)
//                    t = t.down;
//                q = q.down;
//                r = q.right;
//            }
//        }
//    }
//
//    /* ---------------- Deletion -------------- */
//
//    /**
//     * Main deletion method. Locates node, nulls value, appends a
//     * deletion marker, unlinks predecessor, removes associated index
//     * nodes, and possibly reduces head index level.
//     *
//     * Index nodes are cleared out simply by calling findPredecessor.
//     * which unlinks indexes to deleted nodes found along path to key,
//     * which will include the indexes to this node.  This is done
//     * unconditionally. We can't check beforehand whether there are
//     * index nodes because it might be the case that some or all
//     * indexes hadn't been inserted yet for this node during initial
//     * search for it, and we'd like to ensure lack of garbage
//     * retention, so must call to be sure.
//     *
//     * @param okey the key
//     * @param value if non-null, the value that must be
//     * associated with key
//     * @return the node, or null if not found
//     */
//    final V doRemove(Object okey, Object value) {
//        Comparable<? super K> key = comparable(okey);
//        for (;;) {
//            Node[K,V] b = findPredecessor(key);
//            Node[K,V] n = b.next;
//            for (;;) {
//                if (n == null)
//                    return null;
//                Node[K,V] f = n.next;
//                if (n != b.next)                    // inconsistent read
//                    break;
//                Object v = n.value;
//                if (v == null) {                    // n is deleted
//                    n.helpDelete(b, f);
//                    break;
//                }
//                if (v == n || b.value == null)      // b is deleted
//                    break;
//                int c = key.compareTo(n.key);
//                if (c < 0)
//                    return null;
//                if (c > 0) {
//                    b = n;
//                    n = f;
//                    continue;
//                }
//                if (value != null && !value.equals(v))
//                    return null;
//                if (!n.casValue(v, null))
//                    break;
//                if (!n.appendMarker(f) || !b.casNext(n, f))
//                    findNode(key);                  // Retry via findNode
//                else {
//                    findPredecessor(key);           // Clean index
//                    if (head.right == null)
//                        tryReduceLevel();
//                }
//                return (V)v;
//            }
//        }
//    }
//
//    /**
//     * Possibly reduce head level if it has no nodes.  This method can
//     * (rarely) make mistakes, in which case levels can disappear even
//     * though they are about to contain index nodes. This impacts
//     * performance, not correctness.  To minimize mistakes as well as
//     * to reduce hysteresis, the level is reduced by one only if the
//     * topmost three levels look empty. Also, if the removed level
//     * looks non-empty after CAS, we try to change it back quick
//     * before anyone notices our mistake! (This trick works pretty
//     * well because this method will practically never make mistakes
//     * unless current thread stalls immediately before first CAS, in
//     * which case it is very unlikely to stall again immediately
//     * afterwards, so will recover.)
//     *
//     * We put up with all this rather than just let levels grow
//     * because otherwise, even a small map that has undergone a large
//     * number of insertions and removals will have a lot of levels,
//     * slowing down access more than would an occasional unwanted
//     * reduction.
//     */
//    private void tryReduceLevel() {
//        HeadIndex[K,V] h = head;
//        HeadIndex[K,V] d;
//        HeadIndex[K,V] e;
//        if (h.level > 3 &&
//            (d = (HeadIndex[K,V])h.down) != null &&
//            (e = (HeadIndex[K,V])d.down) != null &&
//            e.right == null &&
//            d.right == null &&
//            h.right == null &&
//            casHead(h, d) && // try to set
//            h.right != null) // recheck
//            casHead(d, h);   // try to backout
//    }
//
//    /* ---------------- Finding and removing first element -------------- */
//
//    /**
//     * Specialized variant of findNode to get first valid node.
//     * @return first node or null if empty
//     */
//    Node[K,V] findFirst() {
//        for (;;) {
//            Node[K,V] b = head.node;
//            Node[K,V] n = b.next;
//            if (n == null)
//                return null;
//            if (n.value != null)
//                return n;
//            n.helpDelete(b, n.next);
//        }
//    }
//
//    /**
//     * Removes first entry; returns its snapshot.
//     * @return null if empty, else snapshot of first entry
//     */
//    Map.Entry[K,V] doRemoveFirstEntry() {
//        for (;;) {
//            Node[K,V] b = head.node;
//            Node[K,V] n = b.next;
//            if (n == null)
//                return null;
//            Node[K,V] f = n.next;
//            if (n != b.next)
//                continue;
//            Object v = n.value;
//            if (v == null) {
//                n.helpDelete(b, f);
//                continue;
//            }
//            if (!n.casValue(v, null))
//                continue;
//            if (!n.appendMarker(f) || !b.casNext(n, f))
//                findFirst(); // retry
//            clearIndexToFirst();
//            return new AbstractMap.SimpleImmutableEntry[K,V](n.key, (V)v);
//        }
//    }
//
//    /**
//     * Clears out index nodes associated with deleted first entry.
//     */
//    private void clearIndexToFirst() {
//        for (;;) {
//            Index[K,V] q = head;
//            for (;;) {
//                Index[K,V] r = q.right;
//                if (r != null && r.indexesDeletedNode() && !q.unlink(r))
//                    break;
//                if ((q = q.down) == null) {
//                    if (head.right == null)
//                        tryReduceLevel();
//                    return;
//                }
//            }
//        }
//    }
//
//
//    /* ---------------- Finding and removing last element -------------- */
//
//    /**
//     * Specialized version of find to get last valid node.
//     * @return last node or null if empty
//     */
//    Node[K,V] findLast() {
//        /*
//         * findPredecessor can't be used to traverse index level
//         * because this doesn't use comparisons.  So traversals of
//         * both levels are folded together.
//         */
//        Index[K,V] q = head;
//        for (;;) {
//            Index[K,V] d, r;
//            if ((r = q.right) != null) {
//                if (r.indexesDeletedNode()) {
//                    q.unlink(r);
//                    q = head; // restart
//                }
//                else
//                    q = r;
//            } else if ((d = q.down) != null) {
//                q = d;
//            } else {
//                Node[K,V] b = q.node;
//                Node[K,V] n = b.next;
//                for (;;) {
//                    if (n == null)
//                        return b.isBaseHeader() ? null : b;
//                    Node[K,V] f = n.next;            // inconsistent read
//                    if (n != b.next)
//                        break;
//                    Object v = n.value;
//                    if (v == null) {                 // n is deleted
//                        n.helpDelete(b, f);
//                        break;
//                    }
//                    if (v == n || b.value == null)   // b is deleted
//                        break;
//                    b = n;
//                    n = f;
//                }
//                q = head; // restart
//            }
//        }
//    }
//
//    /**
//     * Specialized variant of findPredecessor to get predecessor of last
//     * valid node.  Needed when removing the last entry.  It is possible
//     * that all successors of returned node will have been deleted upon
//     * return, in which case this method can be retried.
//     * @return likely predecessor of last node
//     */
//    private Node[K,V] findPredecessorOfLast() {
//        for (;;) {
//            Index[K,V] q = head;
//            for (;;) {
//                Index[K,V] d, r;
//                if ((r = q.right) != null) {
//                    if (r.indexesDeletedNode()) {
//                        q.unlink(r);
//                        break;    // must restart
//                    }
//                    // proceed as far across as possible without overshooting
//                    if (r.node.next != null) {
//                        q = r;
//                        continue;
//                    }
//                }
//                if ((d = q.down) != null)
//                    q = d;
//                else
//                    return q.node;
//            }
//        }
//    }
//
//    /**
//     * Removes last entry; returns its snapshot.
//     * Specialized variant of doRemove.
//     * @return null if empty, else snapshot of last entry
//     */
//    Map.Entry[K,V] doRemoveLastEntry() {
//        for (;;) {
//            Node[K,V] b = findPredecessorOfLast();
//            Node[K,V] n = b.next;
//            if (n == null) {
//                if (b.isBaseHeader())               // empty
//                    return null;
//                else
//                    continue; // all b's successors are deleted; retry
//            }
//            for (;;) {
//                Node[K,V] f = n.next;
//                if (n != b.next)                    // inconsistent read
//                    break;
//                Object v = n.value;
//                if (v == null) {                    // n is deleted
//                    n.helpDelete(b, f);
//                    break;
//                }
//                if (v == n || b.value == null)      // b is deleted
//                    break;
//                if (f != null) {
//                    b = n;
//                    n = f;
//                    continue;
//                }
//                if (!n.casValue(v, null))
//                    break;
//                K key = n.key;
//                Comparable<? super K> ck = comparable(key);
//                if (!n.appendMarker(f) || !b.casNext(n, f))
//                    findNode(ck);                  // Retry via findNode
//                else {
//                    findPredecessor(ck);           // Clean index
//                    if (head.right == null)
//                        tryReduceLevel();
//                }
//                return new AbstractMap.SimpleImmutableEntry[K,V](key, (V)v);
//            }
//        }
//    }
//    

    
      /* ------ Map API methods ------ */

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.
     *
     * @param key key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     * 
     * I changed the type here fro Any to comparable, to get rid of the 
     * mysterious comparable method calls.
     */
     def containsKey(key: Comparable[K]) =   doGet(key) != null;
    }
//
//    /**
//     * Returns the value to which the specified key is mapped,
//     * or {@code null} if this map contains no mapping for the key.
//     *
//     * <p>More formally, if this map contains a mapping from a key
//     * {@code k} to a value {@code v} such that {@code key} compares
//     * equal to {@code k} according to the map's ordering, then this
//     * method returns {@code v}; otherwise it returns {@code null}.
//     * (There can be at most one such mapping.)
//     *
//     * @throws ClassCastException if the specified key cannot be compared
//     *         with the keys currently in the map
//     * @throws NullPointerException if the specified key is null
//     */
//    public V get(Object key) {
//        return doGet(key);
//    }
//
//    /**
//     * Associates the specified value with the specified key in this map.
//     * If the map previously contained a mapping for the key, the old
//     * value is replaced.
//     *
//     * @param key key with which the specified value is to be associated
//     * @param value value to be associated with the specified key
//     * @return the previous value associated with the specified key, or
//     *         <tt>null</tt> if there was no mapping for the key
//     * @throws ClassCastException if the specified key cannot be compared
//     *         with the keys currently in the map
//     * @throws NullPointerException if the specified key or value is null
//     */
//    public V put(K key, V value) {
//        if (value == null)
//            throw new NullPointerException();
//        return doPut(key, value, false);
//    }
//
//    /**
//     * Removes the mapping for the specified key from this map if present.
//     *
//     * @param  key key for which mapping should be removed
//     * @return the previous value associated with the specified key, or
//     *         <tt>null</tt> if there was no mapping for the key
//     * @throws ClassCastException if the specified key cannot be compared
//     *         with the keys currently in the map
//     * @throws NullPointerException if the specified key is null
//     */
//    public V remove(Object key) {
//        return doRemove(key, null);
//    }
//
//    /**
//     * Returns <tt>true</tt> if this map maps one or more keys to the
//     * specified value.  This operation requires time linear in the
//     * map size. Additionally, it is possible for the map to change
//     * during execution of this method, in which case the returned
//     * result may be inaccurate.
//     *
//     * @param value value whose presence in this map is to be tested
//     * @return <tt>true</tt> if a mapping to <tt>value</tt> exists;
//     *         <tt>false</tt> otherwise
//     * @throws NullPointerException if the specified value is null
//     */
//    public boolean containsValue(Object value) {
//        if (value == null)
//            throw new NullPointerException();
//        for (Node[K,V] n = findFirst(); n != null; n = n.next) {
//            V v = n.getValidValue();
//            if (v != null && value.equals(v))
//                return true;
//        }
//        return false;
//    }
//
//    /**
//     * Returns the number of key-value mappings in this map.  If this map
//     * contains more than <tt>Integer.MAX_VALUE</tt> elements, it
//     * returns <tt>Integer.MAX_VALUE</tt>.
//     *
//     * <p>Beware that, unlike in most collections, this method is
//     * <em>NOT</em> a constant-time operation. Because of the
//     * asynchronous nature of these maps, determining the current
//     * number of elements requires traversing them all to count them.
//     * Additionally, it is possible for the size to change during
//     * execution of this method, in which case the returned result
//     * will be inaccurate. Thus, this method is typically not very
//     * useful in concurrent applications.
//     *
//     * @return the number of elements in this map
//     */
//    public int size() {
//        long count = 0;
//        for (Node[K,V] n = findFirst(); n != null; n = n.next) {
//            if (n.getValidValue() != null)
//                ++count;
//        }
//        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
//    }
//
//    /**
//     * Returns <tt>true</tt> if this map contains no key-value mappings.
//     * @return <tt>true</tt> if this map contains no key-value mappings
//     */
//    public boolean isEmpty() {
//        return findFirst() == null;
//    }
}