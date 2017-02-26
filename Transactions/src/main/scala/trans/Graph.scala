
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller, Matthew Saltz
 *  @version 1.2
 *  @date    Wed May 13 14:58:25 EDT 2015
 *  @see     LICENSE (MIT style license file).
 *
 *  Graph Data Structure Using Immutable Sets
 */

package trans

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Graph` class stores vertex-labeled directed graphs using an adjacency
 *  set 'ch' representation, e.g., 'ch = { {1, 2}, {0}, {1} }' means that the
 *  graph has the following edges { (0, 1), (0, 2), (1, 0), (2, 1) }.
 *  Optionally, inverse adjacency via the 'pa' array can be stored at the cost
 *  of nearly doubling the storage requirements.
 *----------------------------------------------------------------------------
 *  @param ch       the array of child (adjacency) vertex sets (outgoing edges)
 *  @param label    the array of vertex labels
 *  @param inverse  whether to store inverse adjacency sets (parents)
 *  @param name     the name of graph
 */
class Graph (ch:      Array [Set [Int]] = Array.ofDim (0),
             label:   Array [Int] = Array.ofDim (0),
             inverse: Boolean = false,
             name:    String = "g")
      extends Cloneable 
{

    /** The map from label to the set of vertices with the label
     */
    //val labelMap = buildLabelMap (label)
    var labelMap = Map[Int, Int]()
    /** The adjacency list that we can change...
     */
    var adjList = ch 

    /** The optional array of vertex inverse (parent) adjacency sets (incoming edges)
     */
    val pa = Array.ofDim [Set [Int]] (if (inverse) adjList.size else 0)

    if (inverse) addPar ()                       // by default, don't use 'pa'

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Clone (make a deep copy) of this graph.
     */
    override def clone: Graph = new Graph (adjList.clone, label.clone, inverse)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Add the inverse adjacency sets for rapid accesses to parent vertices.
     */
    def addPar ()
    {
        for (j <- pa.indices) pa(j) = Set [Int] ()
        for (i <- adjList.indices; j <- adjList(i)) pa(j) += i
    } // addPar

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the number of vertices in the graph.
     */
    def size = adjList.size

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the number of edges in the graph.
     */
    def nEdges = adjList.foldLeft (0) { (n, i) => n + i.size }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Given an array of labels, return an index from labels to the sets of
     *  vertices containing those labels.
     *  @param label  the array of vertex labels of type `Int`
     */
    def buildLabelMap (label: Array [Int]): Map [Int, Int] =
    {
        var labelMap = Map [Int, Int] ()
        for (i <- label.indices) {                      // for each vertex i
            val lab  = label(i)                         // label for vertex i
            labelMap += lab -> i
        } // for
        labelMap
    } // buildLabelMap

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the maximum label value.
     */ 
//  def nLabels = labelMap.keys.max

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine the number of vertices in the graph that have outgoing edges
     *  to themselves (self loops).
     */ 
    def nSelfLoops: Int =
    {
        adjList.indices.foldLeft (0) { (sum, i) => if (adjList(i) contains i) sum + 1 else sum }
    } // nSelfLoops

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether 'this' graph is (weakly) connected.
     *
    def isConnected: Boolean = (new GraphDFS (this)).weakComps == 1
     */

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Check whether the end-point vertex id of each edge is within bounds:
     *  0 .. 'maxId'.
     */
    def checkEdges: Boolean =
    {
        val maxId = adjList.size - 1
        for (u <- adjList.indices; u_c <- adjList(u) if u_c < 0 || u_c > maxId) {
            println (s"checkEdges: child of $u, with vertex id $u_c not in bounds 0..$maxId")
            return false
        } // for
        true
    } // checkEdges

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether 'this' graph and graph 'g' have the same vertices
     *  and edges.  Note, this is more strict than graph isomorphism which allows
     *  vertices to be renumbered.
     *  @param g  the other digraph
     */
    def same (g: Graph): Boolean =
    {
        if (size != g.size) return false
	val gCh = g.getEdges()
        for (u <- adjList.indices; u_c <- adjList(u) if ! (gCh(u) contains u_c)) return false
        true
    } // same

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the set of vertices in the graph with label l.
     */
    def getVerticesWithLabel (l: Int) = labelMap.getOrElse (l, -1)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert 'this' graph to a string in a shallow sense.  Large arrays are
     *  not converted.  Use 'print' to show all information.
     */
    override def toString: String =
    {
        s"Graph (adjList.length = ${adjList.length}, label.length = ${label.length}, inverse = $inverse, name = $name)"
    } // toString

   //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert the 'i'th row/line of 'this' graph to a string.
     *  @param i     the 'i'th row/line
     *  @param clip  whether to clip out "Set(" and ")"
     */
    def toLine (i: Int, clip: Boolean = true): String =
    {
        var ch_i = adjList(i).toString
        if (clip) ch_i = ch_i.replace ("Set(", "").replace (")", "")
        if (i < label.length) s"$i, ${label(i)}, $ch_i"
        else                  s"$i, $ch_i"
    } // toLine

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Print 'this' graph in a deep sense with all the information.
     *  @param clip  whether to clip out "Set(" and ")"
     */
    def printG (clip: Boolean = true)
    {
        println (s"Graph ($name, $inverse, $size")
        for (i <- adjList.indices) println (toLine (i, clip))
        println (")")

    } // printG

    def printG2()
    {
	for(key <- labelMap.keys) {
	      var adjListPos = labelMap(key)
	      print(s"$key: ")
	      for( j <- adjList(adjListPos) ) print(s" $j ")
	      println("")
	}
    }
    def hasCycle (): Boolean = 
    {
	val G_N = 0;
	val Y_W = 1;
	val R_D = 2;
	
        val color = Array.fill (adjList.length)(G_N)    // traffic light colors: GreeN, YelloW, ReD
	println(s"color.length: ${color.length}")

        for (v <- color.indices if color(v) == G_N && loopback (v)) return true 

        /*  Search the descendants of vertex 'u' to see if there is a loopback.
         *  @param u  the vertex where the search starts
         */
        def loopback (u: Int): Boolean =
        {
	    println(s"loopback on $u")
            if (color(u) == Y_W) return true
            color(u) = Y_W
            for (vLabel <- adjList(u); v=labelMap(vLabel) if color(v) != R_D && loopback (v)) return true
            color(u) = R_D
            false
        } // loopback

       false
    } // hasCycle

    def addEdge(u: Int, v: Int){
    	val uPos = labelMap(u)
    	adjList(uPos)=adjList(uPos)+v

    }

    def removeEdge(u: Int, v: Int){
    	val uPos = labelMap(u)
    	adjList(uPos)=adjList(uPos)-v
    }

    def addNode(u: Int)
    {
	var newAdjList = Array.ofDim[Set[Int]](adjList.length + 1)
	for( i <- adjList.indices ) newAdjList(i) = adjList(i)
	newAdjList(newAdjList.length-1) = Set[Int]()
	labelMap = labelMap + (u -> (newAdjList.length - 1))
	adjList = newAdjList  
    }

    def getEdges() : Array[Set[Int]] =
    {
	adjList.clone
    }

    def getLabels() : Array[Int] =
    {
	label.clone
    }

    def getName() : String =
    {
	name ++ ""
    }

} // Graph class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Graph` companion object contains the example query graphs.
 */
object Graph
{
    // -----------------------------------------------------------------------
    // Simple data and query graphs.
    // -----------------------------------------------------------------------

    // data graph g1 ---------------------------------------------------------

    val g1 = new Graph (Array (Set (),                      // ch(0)
                               Set (0, 2, 3, 4),            // ch(1)
                               Set (0),                     // ch(2)
                               Set (4),                     // ch(3)
                               Set ()),                     // ch(4)
                        Array (11, 10, 11, 11, 11),         // vertex labels
                        false, "g1")                        // inverse, name

    // query graph q1 --------------------------------------------------------

    val q1 = new Graph (Array (Set (1, 2),                  // ch(0)
                               Set (),                      // ch(1)
                               Set (1)),                    // ch(2)
                        Array (10, 11, 11),
                        false, "q1")

    val g1p = new Graph (g1.getEdges(), g1.getLabels(), true, g1.getName())    // with parents
    val q1p = new Graph (q1.getEdges(), q1.getLabels(), true, q1.getName())    // with parents

    // -----------------------------------------------------------------------
    // Data and query graphs from the following paper:
    // John A. Miller, Lakshmish Ramaswamy, Arash J.Z. Fard and Krys J. Kochut,
    // "Research Directions in Big Data Graph Analytics,"
    // Proceedings of the 4th IEEE International Congress on Big Data (ICBD'15),
    // New York, New York (June-July 2015) pp. 785-794.
    // -----------------------------------------------------------------------

    // data graph g2 ---------------------------------------------------------

    val g2 = new Graph (Array (Set (1),                     // ch(0)
                               Set (0, 2, 3, 4, 5),         // ch(1)
                               Set (),                      // ch(2)
                               Set (),                      // ch(3)
                               Set (),                      // ch(4)
                               Set (6, 10),                 // ch(5)
                               Set (7, 4, 8, 9),            // ch(6)
                               Set (1),                     // ch(7)
                               Set (),                      // ch(8)
                               Set (),                      // ch(9)
                               Set (11),                    // ch(10)
                               Set (12),                    // ch(11)
                               Set (11, 13),                // ch(12)
                               Set (),                      // ch(13)
                               Set (13, 15),                // ch(14)
                               Set (16),                    // ch(15)
                               Set (17, 18),                // ch(16)
                               Set (14, 19),                // ch(17)
                               Set (20),                    // ch(18)
                               Set (14),                    // ch(19)
                               Set (19, 21),                // ch(20)
                               Set (),                      // ch(21)
                               Set (21, 23),                // ch(22)
                               Set (25),                    // ch(23)
                               Set (),                      // ch(24)
                               Set (24, 26),                // ch(25)
                               Set (28),                    // ch(26)
                               Set (),                      // ch(27)
                               Set (27, 29),                // ch(28)
                               Set (22)),                   // ch(29)
                       Array (10, 11, 12, 12, 12, 10, 11, 10, 12, 15, 12, 10, 11, 12, 11,
                              10, 11, 12, 10, 10, 11, 12, 11, 10, 12, 11, 10, 12, 11, 10),
                              false, "g2")

    // query graph q2 --------------------------------------------------------

    val q2 = new Graph (Array (Set (1),                     // ch(0)
                               Set (0, 2, 3),               // ch(1)
                               Set (),                      // ch(2)
                               Set ()),                     // ch(3)
                        Array (10, 11, 12, 12),
                               false, "q2")

    val g2p = new Graph (g2.getEdges(), g2.getLabels(), true, g2.getName())    // with parents
    val q2p = new Graph (q2.getEdges(), q2.getLabels(), true, q2.getName())    // with parents

} // Graph object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `GraphTest` object is used to test the `Graph` class using the graphs
 *  given in the `Graph` companion object.
 *  > run-main trans.GraphTest
 */
object GraphTest extends App
{
    import Graph._
    g1.printG ()
    q1.printG ()
    g2.printG ()
    q2.printG ()

    val g3 = new Graph()
    println("Adding nodes to new graph")
    g3.addNode(1)
    g3.addNode(2)
    g3.addNode(3)

    println("Adding edges to new graph")
    g3.addEdge(1,2)
    g3.addEdge(1,3)
    g3.addEdge(2,3)

    println("Before adding cycle: ")
    g3.printG2()

    g3.addEdge(3,1)
    println("After adding cycle: ")
    g3.printG2()

    println(s"Cycle detection: ${g3.hasCycle()}")
} // GraphTest object

