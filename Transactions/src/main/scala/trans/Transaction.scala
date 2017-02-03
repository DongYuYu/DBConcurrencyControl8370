/*TODO

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
import scala.collection.mutable.{Map,Set}
import java.util.concurrent.locks.ReentrantReadWriteLock
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
    private val DEBUG       = true						// debug flag
    private val tid         = nextCount ()        		    		// tarnsaction identifier
    private var rw_set      = Map[Int, (Int, Int)]()	    			// the read write set
    private var numOps      = 0	   	 		    			// [ oid, (num_reads,num_writes)]
    private var contracting = false						// keeps track of which 2PL phase we're in (contracting or expanding)
    private var readLocks   = Set[ (Int, ReentrantReadWriteLock.ReadLock)]()	// set of read locks we haven't unlocked yet and the oid they apply to
    private var writeLocks  = Set[ (Int, ReentrantReadWriteLock.WriteLock)]() 	// set of write locks we haven't unlocked yet and the oid they apply to
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
     */
    override def run ()
    {
    	for(i <- sch.indices){
	      val op = sch(i)
	      var tup = rw_set.get(op._3)
	      if( tup == None ) {
	      	  var newTupe = (0,0)
		  if( op._1 == r )
		      newTupe = (1,0)
		  else
			newTupe= (0,1)
		  rw_set += (op._3 -> newTupe)
	      } // if
	      else{
		if( op._1 == r ) rw_set(op._3) = tup.get.copy(_1 = tup.get._1+1)
		else rw_set(op._3) = tup.get.copy(_2 = tup.get._2+1)
	      } // else
	      numOps += 1
	}// for

	print("beginning\n")
        begin ()
        for (i <- sch.indices) {
	    println("for")
            val op = sch(i)
            println (sch(i))
	    println("for")
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
	println("reading");
	var ret  = Array.ofDim[Byte](128)
	var lock = LockTable.lock( oid )				// get the rrwl associated with this object from the lock table
	var prime_lock = lock.writeLock()				// get the writeLock associated with the rrwl
	if( prime_lock.isHeldByCurrentThread() ){			// if you already hold the write lock, start reading
	    ret = (VDB.read (tid,oid))._1
	} // if
	else if( (rw_set contains oid) && (rw_set(oid)._2>0) ){		// if you will need to write this item in the future, use the writeLock for read
	     prime_lock.lock()	     					// try to lock the write lock
	     ret = (VDB.read (tid,oid))._1
	     writeLocks add (oid,prime_lock)
	} // else if
	else{
		var prime_lock2 = lock.readLock()			// switch to the read lock b/c you don't need to write in the future
		prime_lock2.lock() 					// try to lock the read lock
		ret = (VDB.read(tid,oid))._1
		readLocks add (oid,prime_lock2)
	} // else
	
	rw_set(oid)=rw_set(oid).copy(_1 = rw_set(oid)._1-1)		// take a read of this object off of the rw_set
	numOps -= 1			  				// update your op counter
	if(numOps == 0) contracting = true				// update your phase field
	if(contracting) releaseReadLocks()				// release your read locks if you can
	ret
    } // read

    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Unlock any read locks this transaction may own.
     */
     def releaseReadLocks()
     {
	for( lock <- readLocks ){
	     lock._2.unlock
	     LockTable.checkLock(lock._1)
	}
     } // releaseReadLocks


    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Unlock any write locks this transaction may own.
     */
     def releaseWriteLocks()
     {
	for( lock <- writeLocks ){
	     lock._2.unlock
	     LockTable.checkLock(lock._1)
	}
     } // releaseWriteLocks

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
    VDB.initCache()
//    t1.start ()
    t2.start ()

} // TransactionTest object

