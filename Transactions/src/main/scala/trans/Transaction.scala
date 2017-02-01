/*TODO
Medhi: implement look ahead locking:
       1. declare read/write set : Map: (oid, (num_reads,writes))
       2. on each read operation
       	  a. do we own the write lock? If so, read?
	  b. if we don't own the write lock, see if you will need to write in the future. If so, pick up write lock. Then read. 
	  c. if we don't need to write in the future, get a read lock. 
       3. on each read/write operation decrement the map value for the oid key
Nick: implement expanding / shrinking phase detection
	  1. keep a flag: 0 ==> expanding, 1 ==> shrinking
	  2. after evey read check if we're expanding or shrinking.
	  3. if we're shrinking, release read locks. (don't release write locks until commit)
	  4. release all write locks at the commit phase.
	     writeLocks = ArrayList[oid: Int]
	     for i <- writeLocks.indices{
	     	 LockTable.get(writeLocks(i)).unlock()
	     }
implement PDB.init_store
*/
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

import Operation._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` companion object
 */
object Transaction
{
    private var count = -1

    def nextCount () = { count += 1; count }

    VDB.initCache ()

} // Transaction object

import Transaction._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
 *  @param sch  the schedule/order of operations for this transaction.
 */
class Transaction (sch: Schedule) extends Thread
{
    private val DEBUG = true              // debug flag
    private val tid = nextCount ()        // tarnsaction identifier

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
     */
    override def run ()
    {
        begin ()
        for (i <- sch.indices) {
            val op = sch(i)
            println (sch(i))
            if (op._1 == r) read (op._3)
            else            write (op._3, VDB.str2record (op.toString))
        } // for
        commit ()
    } // run

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
     *  @param oid  the object/record being read
     */
    def read (oid: Int): VDB.Record =
    {
	LockTable.read_lock( tid, oid).lock()
        VDB.read (tid, oid)._1
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
     *  @param oid    the object/record being written
     *  @param value  the new value for the the record
     */
    def write (oid: Int, value: VDB.Record)
    {
        VDB.write (tid, oid, value)
    } // write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin this transaction.
     */
    def begin ()
    {
        VDB.begin (tid)
    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit this transaction.
     */
    def commit ()
    {
        VDB.commit (tid)
        if (DEBUG) println (VDB.logBuf)
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
     */
    def rollback ()
    {
        VDB.rollback (tid)
    } // rollback

} // Transaction class


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
 *  > run-main trans.TransactionTest
 */
object TransactionTest extends App
{
    val t1 = new Transaction (new Schedule (List ( (r, 0, 0), (r, 0, 1), (w, 0, 0), (w, 0, 1) )))
    val t2 = new Transaction (new Schedule (List ( (r, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) )))

    t1.start ()
    t2.start ()

} // TransactionTest object

