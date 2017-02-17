/*A
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

import scala.collection.mutable.{ArrayBuffer, Map, Set}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.Breaks._
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

import scalation.graphalytics.Graph
import scalation.graphalytics.Cycle.hasCycle

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
  *  @param sch  the schedule/order of operations for this transaction.
  */
class Transaction (sch: Schedule, concurrency: Int =0) extends Thread
{
    private val DEBUG       = true						// debug flag
    val tid         = nextCount ()        		    		// tarnsaction identifier
    private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set : [oid, (num_reads, num_writes)]
    private var contracting = false						// keeps track of which 2PL phase we're in (contracting or expanding)
    private var readLocks   = Map [Int, ReentrantReadWriteLock.ReadLock ]()	// (oid -> readLock)  read locks we haven't unlocked yet
    private var writeLocks  = Map [Int, ReentrantReadWriteLock.WriteLock]() 	// (oid -> writeLock) write locks we haven't unlocked yet 
    private val READ        = 0
    private val WRITE       = 1
    private val _2PL        = 0
    private val TSO         = 1
    private var ROLLBACK    = false


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
      */
    override def run ()
    {
    	if(concurrency == _2PL) fillReadWriteSet()
        breakable{
		for (i <- sch.indices) {
		    //println(s"${sch(i)}")
	    	    if(!ROLLBACK){
			val (op,tid,oid) = sch(i)
            	    	//if(DEBUG) println (sch(i))
            	    	if (op == r) read (oid)
            	    	else         write (oid, VDB.str2record (sch(i).toString))
	    	    } // if
	    	    else break   
        	} // for
	} // breakable
        if(!ROLLBACK) commit ()
    } // run

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Fills the read/write set for this transaction.
      */
    def fillReadWriteSet()
    {
	for(i <- sch.indices){
	      val (op,tid,oid) = sch(i)
	      if (rwSet contains oid){
	      	 if( op == r ) rwSet(oid)(READ)  += 1		//increment the read value for this object in the readWriteSet
		 else          rwSet(oid)(WRITE) += 1		//increment the write value
	      } // if
	      else{
		var tup = Array.fill(2)(0)				//add a new member to the read write set
		if( op == r ) tup(READ) = 1				//make it a read member
		else          tup(WRITE) = 1				//make it a write member
		
		rwSet += (oid -> tup) 
	      } // else
	}// for
    } // fillReadWriteSet

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      *  @param oid  the object/record being read
      */
    def read2PL (oid: Int): VDB.Record =
    {
        //if(concurrency ==2PL) read2PL(oid)
        //else readTSO(oid)
        println("reading")
        var lock = new ReentrantReadWriteLock()
        var waitingTid =0
        var ret   = Array.ofDim[Byte](128)
        LockTable.synchronized{
            lock  = LockTable.lock( oid,tid )				// get the rrwl associated with this object from the lock table
            waitingTid = LockTable.table.getOrElse(oid,(null,-1))._2
        }

        var prime_lock = lock.writeLock()				// get the writeLock associated with the rrwl
        if( prime_lock.isHeldByCurrentThread() ){			// if you already hold the write lock, start reading
            ret = (VDB.read (tid,oid))._1
        } // if
        else if( (rwSet contains oid) && (rwSet(oid)(1)>0) ){		// if you will need to write this item in the future, use the writeLock for read

            VDB.ch(tid)+= waitingTid
            val graph = new Graph(VDB.ch)                       //if wait for lock will cause a deadlock then roll back and clear the graph assoicated to the tid
            if (hasCycle(graph)) {
                VDB.ch(tid)= VDB.ch(tid).empty
                rollback()
            }

            prime_lock.lock()	     					// try to lock the write lock
            LockTable.table(oid)=(lock,tid)
            ret = (VDB.read (tid,oid))._1
            writeLocks += (oid -> prime_lock)
        } // else if
        else{
            var prime_lock2 = lock.readLock()			// switch to the read lock b/c you don't need to write in the future
            if (lock.isWriteLocked) {
                VDB.ch(tid)+= waitingTid
                val graph = new Graph(VDB.ch)
                if (hasCycle(graph)) {
                    VDB.ch(tid)= VDB.ch(tid).empty
                    rollback()
                }
            }
            prime_lock2.lock() 					// try to lock the read lock
            ret = (VDB.read(tid,oid))._1
            readLocks += (oid -> prime_lock2)
        } // else

        rwSet(oid)(READ) -= 1						// take a read of this object off of the rw_set

        if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                (rwSet(oid)(WRITE) == 0) ) rwSet -= oid

        ret
    } // read


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write2PL (oid: Int, value: VDB.Record)
    {

        var lock = LockTable.lock(oid,tid)
        var waitingTid = LockTable.table.getOrElse(oid,(null,-1))._2
        var primeLock = lock.writeLock()

        if(primeLock.isHeldByCurrentThread) VDB.write(tid, oid, value)
        else{
            VDB.ch(tid)+= waitingTid
            val graph = new Graph(VDB.ch)
            if (hasCycle(graph)) {                                                      //if wait for lock will cause a deadlock then roll back and clear the graph assoicated to the tid
                VDB.ch(tid)= VDB.ch(tid).empty
                rollback()
            }
            primeLock.lock()
            LockTable.table(oid)=(lock,tid)
            writeLocks += (oid -> primeLock)
            VDB.write(tid, oid, value)
        }
        rwSet(oid)(WRITE) -= 1
        if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                (rwSet(oid)(WRITE) == 0) ) rwSet -= oid
    }
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */
    def readTS (oid: Int): VDB.Record =
    {
	var rec		= Array.ofDim[Byte](128)
	val writeTS 	= VDB.tsTable(oid)(1)
	val readTS	= VDB.tsTable(oid)(0)
	val lock 	= LockTable.lock(oid, tid)
	val readLock 	= lock.readLock()

        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
		if(writeTS < tid){							// check for STSO, then use locks
			if( writeLocks contains oid ){					// read 'em if you got 'em
			    rec = VDB.read(tid,oid)._1
			    if(readTS < tid) VDB.tsTable(oid)(0) = tid
			} // if				
			else{								// THIS SHOULD HAPPEN
	    			readLock.lock()             // lock the readLock
                LockTable.table(oid)=(lock,tid)
				rec = VDB.read(tid, oid)._1
				if(readTS < tid) VDB.tsTable(oid)(0) = tid
				readLock.unlock()
			} // else
		} // if
            	else {
			rec = VDB.read(tid,oid)._1
			if(readTS < tid) VDB.tsTable(oid)(0) = tid
		}
	}
        else rollback()									// rollback if the current TS is bigger than write_TS,
	     										// which means some yonger Tj has written oid  	    
	rec
    } // readTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO (oid: Int, value: VDB.Record)
    {
	val readTS  = VDB.tsTable(oid)(READ)
	val writeTS = VDB.tsTable(oid)(WRITE)
        if(tid < readTS || tid < writeTS) rollback()			
	else {
	     if( tid > writeTS ){						// check for STSO
	     	 val lock      = LockTable.lock(oid, tid)
		 val writeLock = lock.writeLock()
		 if( writeLocks contains oid ) {				// SHOLDN'T HAPPEN write 'em if you got 'em
		     VDB.write(tid,oid,value)  					// writeLocks contains oid => we were last to write to this object => writeTS == tid
		     VDB.tsTable(oid)(WRITE) = tid				// SHOULD NEVER BE HERE
		 } // if		
		 else{								// i.e. - we don't have the lock yet
		 		 writeLock.lock()				// else lock before writing
                 LockTable.table(oid)=(lock,tid)
		 		 VDB.write(tid,oid,value)
		 		 writeLocks += (oid -> writeLock)		// add the write lock to your set
				 VDB.tsTable(oid)(WRITE) = tid
		 } // else
	     } // if
	     else VDB.write(tid,oid,value)    
	}
	
    } // writeTSO


    //::
    /*
    **/
    def releaseReadLocks()
    {
	println(s"releasing read locks for tid: $tid")
	for(i <- readLocks.keys) {
	      println(s"releasing readLock for oid: ${i}")
	      readLocks(i).unlock()
	} // for
    }


    //::
    /*
    **/
    def releaseWriteLocks()
    {
	println(s"releasing writeLocks for tid: $tid")
	for(i <- writeLocks.keys) {
	      println(s"releasing writeLock for oid: ${i}")
	      writeLocks(i).unlock()
	} // for
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'. Redirect to different concurrency by ConcurrencyFlag setting
      *  @param oid  the object/record being read
      */
    def read (oid: Int) :VDB.Record ={
        if (concurrency == TSO) readTS(oid)
        else read2PL(oid)
    }
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'. Redirect to different concurrency control by ConcurrencyFlag setting
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write (oid:Int, value:VDB.Record  ) ={
        if (concurrency == TSO ) writeTSO(oid, value)
        else write2PL (oid, value)
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin this transaction.
      */
    def begin ()=
    {
        VDB.begin (tid)
    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit this transaction.
      */
    def commit ()
    {
        VDB.commit (tid)
        //if (DEBUG) println (VDB.logBuf)
	releaseReadLocks()
	releaseWriteLocks()
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
      */
    def rollback ()
    {
	println("rollback...")
	ROLLBACK = true
        VDB.rollback (tid)
        releaseReadLocks()
        releaseWriteLocks()
	val newT = new Transaction(this.sch, this.concurrency)
	newT.start()

    } // rollback

    
} // Transaction class

object waitGraph{


}
class waitGraph  extends Thread {

    import scalation.graphalytics.Graph
    import scala.collection.immutable.Set
    import scalation.graphalytics.Cycle.hasCycle

    var k = "dong"
    //  var waitGraph =Array.ofDim[Set[Int]](nTrans)
    //  for (i <- waitGraph.indices) waitGraph(i) = Set[Int]()

    //  var g = new Graph(waitGraph)

    override def run() {

        var lock = new adjustLock()
        lock.writeLock().lock()

        println(lock.getOwner.getClass.getDeclaredField("k").get(this))


    }}
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  * > run-main trans.TransactionTest
  */
object TransactionTest extends App {
    private val _2PL = 0
    private val TSO = 1
    private val numTrans = 20
    private val numOps   = 20
    private val numObjs  = 20
    
    //val t1 = new Transaction (new Schedule (List ( ('r', 0, 0), ('r', 0, 1), (w, 0, 0), (w, 0, 1) )),TSO)
    //val t2 = new Transaction (new Schedule (List ( ('r', 1, 0), ('r', 1, 1), (w, 1, 0), (w, 1, 1) )),TSO)


    //generate transactions

    val transactions = Array.ofDim[Transaction](numTrans)
    for (i <- transactions.indices) transactions(i) = new Transaction(Schedule.genSchedule2(i,numOps,numObjs),TSO)
    VDB.initCache()
    for (i <- transactions.indices) transactions(i).start()


} // TransactionTest object
