
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.1
 *  @date    Tue Jan 10 14:34:43 EST 2017
 *  @see     LICENSE (MIT style license file).
 *------------------------------------------------------------------------------
 *  Instructions:
 *      Download sbt
 *      Download transactions.zip
 *      unzip transactions.zip
 *      cd transactions
 *      sbt
 *      > compile
 *      > run-main trans.ScheduleTest
 *      > exit
 */

package trans

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Operation` object defines the 'Op' type and read (r) and write (w) operation-types.
 */
object Operation
{
    type Op = (Char, Int, Int)                        // 3-tuple (r/w, tid, oid)
    val r = 'r'                                       // r -> read
    val w = 'w'                                       // w -> write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether 'o' and 'p' are conflicting operations (at least one write,
     *  different transactions, same data object.
     *  @param o  the first operation
     *  @param p  the second operation
     */
    def conflicts (o: Op, p: Op): Boolean =
    {
        (o._1 == w || p._1 == w) && o._2 != p._2 && o._3 == p._3
    } // conflicts

} // Operation object

import Operation._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Schedule` class represents a transaction schedule as a list of operations,
 *  where each operation is a 3-tuple (operation-type, transaction-id, data-object-id).
 */
class Schedule (s: List [Op])
{
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether this schedule is Conflict Serializable (CSR).
     *  @see www.cs.uga.edu/~jam/scalation_1.2/src/main/scala/scalation/graphalytics/Cycle.scala
     *  @param nTrans  the number of transactions in this schedule
     */
    def isCSR (nTrans: Int): Boolean =
    {
        val g = new Graph (Array.fill (nTrans)(Set [Int] ()))
        for (i <- s.indices; j <- i+1 until s.size if conflicts (s(i), s(j))) g.ch(s(i)._2) += s(j)._2
        g.printG ()
        // I M P L E M E N T
        false
    } // isCSR

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Determine whether this schedule is View Serializable (VSR).
     *  @param nTrans  the number of transactions in this schedule
     */
    def isVSR (nTrans: Int): Boolean =
    {
        // I M P L E M E N T - bonus
        false
    } // isVSR

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the ith operation in the schedule.
     *  @param i  the index value
     */
    def apply (i: Int): Op = s(i)

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Return the range for the schedule indices.
     */
    def indices: Range = 0 until s.size

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Iterate over the schedule element by element.
     *  @param f  the function to apply
     */
    def foreach [U] (f: Op => U)
    {
        s.foreach (f)
    } // foreach

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert the schedule to a string.
     */
    override def toString: String =
    {
        s"Schedule ( $s )".replace ("List(", "").replace (") )", " )")
    } // toString:

} // Schedule class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Schedule` companion object provides factory methods for building schedules.
 */
object Schedule
{
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Randomly generate a schedule.
     *  @param nTrans  the number of transactions
     *  @param nOps    the number of operations per transaction
     *  @param nObjs   the number of data objects accessed
     */
    def gen (nTrans: Int, nOps: Int, nObjs: Int): Schedule =
    {
        val total = nTrans * nOps
        val s = (for (i <- 0 until total) yield (w, 0, 0)).toList
        // F I X  I M P L E M E N T A T I O N - randomize the 3-tuples
        new Schedule (s)
    } // gen

} // Schedule object


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `ScheduleTest` object is used to test the `Schedule` class.
 *  > run-main trans.ScheduleTest
 */
object ScheduleTest extends App
{
    val s1 = new Schedule (List ( (r, 0, 0), (r, 1, 0), (w, 0, 0), (w, 1, 0) ))
    val s2 = new Schedule (List ( (r, 0, 0), (r, 1, 1), (w, 0, 0), (w, 1, 1) ))

    println (s"s1 = $s1 is CSR? ${s1.isCSR (2)}")
    println (s"s1 = $s1 is VSR? ${s1.isVSR (2)}")
    println (s"s2 = $s2 is CSR? ${s2.isCSR (2)}")
    println (s"s2 = $s2 is VSR? ${s2.isVSR (2)}")

    println (s"s3 = ${Schedule.gen (3, 2, 2)}")

} // ScheduleTest object

