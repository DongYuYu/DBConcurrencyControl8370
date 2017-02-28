
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

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
  *  @param sch  the schedule/order of operations for this transaction.
  */
class Transaction (sch: Schedule, concurrency: Int =0) extends Thread
{
    private val DEBUG       = true						// debug flag
    private val tid         = nextCount ()        		    		// tarnsaction identifier
    private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set : [oid, (num_reads, num_writes)]
    private var readLocks   = Map [Int, ReentrantReadWriteLock.ReadLock ]()	// (oid -> readLock)  read locks we haven't unlocked yet
    private var writeLocks  = Map [Int, ReentrantReadWriteLock.WriteLock]() 	// (oid -> writeLock) write locks we haven't unlocked yet 
    private val READ        = 0
    private val WRITE       = 1
    private val _2PL        = 0
    private val TSO         = 1
    private var ROLLBACK    = false
    private val debugSynch  = true

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
      */
    override def run ()
    {
	begin()
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
	println(s"from trans: read($tid,$oid)")
	var rec		= Array.ofDim[Byte](128)
        var lock        = LockTable.getObjLock( oid )					   // get the rrwl associated with this object from the lock table
        var writeLock   = lock.writeLock()	    					   // get the writeLock associated with the rrwl
	var writeLocked = false
	var futureWrite = (rwSet contains oid) && rwSet(oid)(1) > 0
	var noDeadlock  = false
	var readOrWrite = READ
	var notLocked   = true
	if(debugSynch) println(s"$tid entering LockTable synch block in read2PL")
	LockTable.synchronized{
		if(debugSynch) println(s"$tid entered LockTable synch block in read2PL")
		writeLocked = lock.isWriteLocked()
		if(futureWrite) readOrWrite = WRITE 
		noDeadlock = WaitsForGraph.checkForDeadlocks(tid, oid, lock, readOrWrite)		   // check to make sure this read request doesn't cause a deadlock
		if ( noDeadlock ){								   // if no deadlocks, try to lock
		     if ( writeLock.isHeldByCurrentThread() ) {				   	   // if you already own the writeLock so go ahead and read
	      		   println(s"$tid is reading oid because it already holds the write lock")
			   notLocked = false
		     } // if
		     else if( LockTable.getOwners(oid).size == 0){					// if nobody owns no deadlock occurs, so try to lock
			   println(s"$tid is reading $oid because it is unlocked")
			   if( futureWrite ) {
			       println(s"$tid wants to writeock $oid because of future write")
			       writeLockObj(oid,lock)
			       println(s"$tid got to writeLock $oid for reading/future write")
			   }
			   else {
			   	println(s"$tid wants to readLock $oid")
			   	readLockObj(oid,lock)
				println(s"$tid got to readLock $oid")
			   } // else
			   notLocked = false
		     } // else if
		     else if( !writeLocked && !futureWrite) {
			      println(s"from read2PL $tid wants to readLock $oid")
			      var owners = LockTable.getOwners(oid)
			      if( owners contains tid ) println("$tid already owns lock for $oid, don't lock an object you've already locked.")
			      else{
				readLockObj(oid, lock)
			      	println(s"from read2PL $tid got to readLock $oid b/c it is shared locked and $tid didn't own a lock for it")
			      } // else
			      notLocked = false
			  } // else if
	   	} // if
		else{
		    rollback()
		} 
	} // synch
	if(debugSynch) println(s"$tid exited LockTable synch block in read2PL")			// get here means no deadlock, but will have to wait
	if( futureWrite && notLocked && !ROLLBACK) {					// else if you will want the write lock in the future 
	   	println(s"$tid wants to writeLock $oid for reading")
	   	writeLockObj(oid,lock)
		println(s"$tid got to writeLock $oid for reading")
	   } // if							
	else if (notLocked && !ROLLBACK){									// else lock for regular reading
	   	println(s"$tid wants to readLock $oid")
	   	readLockObj(oid,lock)
		println(s"$tid got to readLock $oid")
	} // else
	if (!ROLLBACK){
		houseKeeping(oid,READ)
		rec = VDB.read (tid,oid)._1
	}
	rec
    } // read2PL

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write2PL (oid: Int, value: VDB.Record)
    {
	println(s"from trans: write($tid,$oid)")
        var lock        = LockTable.getObjLock( oid )					   // get the rrwl associated with this object from the lock table
        var writeLock   = lock.writeLock()	    					   // get the writeLock associated with the rrwl
	var writeLocked = false
	var futureWrite = (rwSet contains oid) && rwSet(oid)(1) > 0
	var noDeadlock  = false
	var notLocked   = true
	if(debugSynch) println(s"$tid entering LockTable synch block in write2PL")
	LockTable.synchronized{
		if(debugSynch) println(s"$tid entered LockTable synch block in write2PL")
		writeLocked = lock.isWriteLocked()
		noDeadlock = WaitsForGraph.checkForDeadlocks(tid, oid, lock, WRITE)		   // check to make sure this read request doesn't cause a deadlock
		if ( noDeadlock ){								   // if no deadlocks, try to lock
		     if ( writeLock.isHeldByCurrentThread() ) {				   	   // if you already own the writeLock so go ahead and read
	      		   println(s"$tid is writing oid because it already holds the write lock")
			   notLocked = false
		     } // if
		     else if( LockTable.getOwners(oid).size == 0){					// if nobody owns no deadlock occurs, so try to lock
			   println(s"$tid is writing $oid because it is unlocked")
			   if(lock.isWriteLocked() || lock.getReadLockCount() > 0 ) println("lock disagrees with table.") // sanity check
			   writeLockObj(oid,lock)
			   notLocked = false
		     } // else if
	   	} // if
		else{
		    rollback()
		} 
	} // synch
	if(debugSynch) println(s"$tid exited LockTable synch block in write2PL")				// get here means no deadlock, but will have to wait
	if(notLocked && !ROLLBACK){
		println(s"$tid wants to writeLock $oid")
		writeLockObj(oid,lock)
		println(s"$tid got to writeLock $oid")
	} // if
	if(!ROLLBACK){
		houseKeeping(oid,WRITE)
		VDB.write (tid,oid, value)
	}
    } // write2PL    


    //::
    /**
     */
    def houseKeeping(oid: Int, readOrWrite: Int)
    {
	rwSet(oid)(readOrWrite) -= 1
	if( (rwSet(oid)(READ) == 0) && (rwSet(oid)(WRITE)==0) ) rwSet -= oid 
    }
   
    //::
    /**
     */
    def writeLockObj(oid: Int, lock: ReentrantReadWriteLock)
    {
	val writeLock = lock.writeLock()
	var notLocked   = true
	if (debugSynch) println(s"$tid is entering locktable synch block in writeLockObj")
	LockTable.synchronized{
		if(debugSynch) println(s"$tid entered LockTable synch block in writeLockObj")
		//println(s"transaction $tid entered synchronized block to writeLockObj")
		val owners = LockTable.getOwners(oid)
		if( owners.size == 0 ) {
		    if( lock.isWriteLocked() || lock.getReadLockCount() > 0)
		    	println(s"$tid trying to xlock on $oid already xlocked, but you think it's free, i.e. owners.size == 0 but lock has lockers...")
		    LockTable.addOwner(oid,tid) 
		    writeLock.lock()
		    writeLocks += (oid -> writeLock)
		    notLocked = false
		} // if
		else if( lock.writeLock().isHeldByCurrentThread() ) notLocked = false
	} // synchronized
	if(debugSynch) println(s"$tid exited LockTable synch block in writeLockObj")
	//println(s"transaction $tid has left synchronized block to writeLockObj")
	if( notLocked ){
	    println(s"$tid is waiting to lock the writeLock for $oid outside of synch block.")
	    writeLock.lock()
	    LockTable.addOwner(oid,tid)
	    println(s"$tid has writelocked $oid in writeLockObj")
	    //writeLockObj(oid, lock)
	} // if
    } // writeLockObj()
    

    def readLockObj(oid: Int, lock: ReentrantReadWriteLock) 
    {
	val writeLocked = lock.isWriteLocked()
	val readLock    = lock.readLock()
	var notLocked = true
	if(debugSynch) println(s"$tid entering LockTable synch block in readLockObj")
	LockTable.synchronized{
		if(debugSynch) println(s"$tid entered LockTable synch block in readLockObj")
		//println(s"transaction $tid entered synchronized block to readLockObj")	
		val owners = LockTable.getOwners(oid)
		if( owners.size == 0 || !writeLocked) {
		    if(owners.size == 0) {
		    		   println(s"from readLockObj $tid is readlocking $oid because there are no owners")
				   if (lock.getReadLockCount() > 0) println(s"$tid thinks no owners on this lock, MISTAKE.") 
		    }
		    else if(!writeLocked) println(s"from readLockObj $tid is readLocking $oid because it is shared locked")
		    LockTable.addOwner(oid, tid)
		    readLock.lock()
		    readLocks += (oid -> readLock)
		    notLocked = false
		} // if
	} // synchronized						// get here means owners != None
	if(debugSynch) println(s"$tid exited LockTable synch block in readLockObj")
	if(notLocked){
	    println(s"$ tid is waiting outside of synch block to read $oid because it was writeLocked")
	    readLock.lock()
	    LockTable.addOwner(oid,tid)
	    readLockObj(oid,lock)
	} // if
    }


    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */
    def readTSO (oid: Int): VDB.Record =
    {
	var rec		= Array.ofDim[Byte](128)
	val writeTS 	= TSTable.writeTS(oid)
	val readTS	= TSTable.readTS(oid)
        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
	   rec = VDB.read(tid,oid)._1
	   if(readTS < tid) TSTable.readStamp(tid,oid)	
	} // if
	else rollback()
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
	val readTS  = TSTable.readTS(oid) 
	val writeTS = TSTable.writeTS(oid)
      
	if( tid > readTS && tid > writeTS ){
	    VDB.write(tid,oid,value)
	    TSTable.writeStamp(tid,oid)	
	}
	else if( tid > readTS ){				//Write's rule
	     VDB.write(tid,oid,value)
	     TSTable.writeStamp(tid,oid)	
	}
	else rollback()
	
	
    } // writeTSO
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */
    def readTSO2 (oid: Int): VDB.Record =
    {
	var rec		= Array.ofDim[Byte](128)
	val writeTS 	= TSTable.writeTS(oid)
	val readTS	= TSTable.readTS(oid)
	val lock 	= LockTable.getObjLock(oid)
	val readLock 	= lock.readLock()
        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
		if(writeTS < tid){							// check for STSO, then use locks
			if( writeLocks contains oid ){					// read 'em if you got 'em
			    rec = VDB.read(tid,oid)._1
			    if(readTS < tid) TSTable.readStamp(tid,oid)
			} // if				
			else{								// THIS SHOULD HAPPEN
				//TODO Implement
			} // else
		} // if
            	else {

			rec = VDB.read(tid,oid)._1
			if(readTS < tid) TSTable.readStamp(tid,oid) 
		} //  else
	}
	else rollback()
	rec
    } // readTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO2 (oid: Int, value: VDB.Record)
    {
	val readTS  = TSTable.readTS(oid)
	val writeTS = TSTable.writeTS(oid)
        if(tid < readTS || tid < writeTS) rollback()			
	else {
	     if( tid > writeTS ){						// check for STSO
	     	 val lock      = LockTable.getObjLock(oid)
		 val writeLock = lock.writeLock()		
		 // TODO Implement
	     } // if
	     else{
		VDB.write(tid,oid,value)
	      	TSTable.writeStamp(tid,oid)
	     }
	}
    } // writeTSO
    

    //::
    /*
    **/
    def releaseReadLocks()
    {
	println(s"releasing read locks for tid: $tid")
	//println(s"transaction $tid is entering synchronized block to releaseReadLocks")	
	LockTable.synchronized{
		WaitsForGraph.synchronized{
			for(oid <- readLocks.keys) {
	      			println(s"$tid releasing readLock for ${oid}")
	      			var readLock = readLocks(oid)
				readLock.unlock()
	      			LockTable.unlock(tid,oid)
	      			readLocks -= oid
			} // for
		} // synch
		//println(s"transaction $tid entered synchronized block to releaseReadLocks")
	}
	//println(s"transaction $tid has left synchronized block to releaseReadLocks")
    }

    //::
    /*
    **/
    def releaseWriteLocks()
    {
	println(s"releasing write locks for tid: $tid")
	if(debugSynch) println(s"transaction $tid entering synchronized block to realeaseWriteLocks.")
	LockTable.synchronized{
		if(debugSynch) println(s"transaction $tid entered synchronized block to releaseWriteLocks")
		WaitsForGraph.synchronized{
			for(oid <- writeLocks.keys) {
	      			println(s"$tid is releasing writeLock for ${oid}")
	      			var writeLock = writeLocks(oid)
				writeLock.unlock()
	      			LockTable.unlock(tid,oid)
	      			readLocks -= oid
			} // for
		} // synch
		
	}
	//println(s"transaction $tid has left synchronized block to releaseWriteLocks")
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'. Redirect to different concurrency by ConcurrencyFlag setting
      *  @param oid  the object/record being read
      */
    def read (oid: Int) :VDB.Record ={
        if (concurrency == TSO) readTSO(oid)
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
	println(s"$tid committed from transaction")
	println(s"Lock table after $tid commit: $LockTable")
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

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  * > run-main trans.TransactionTest
  */

object TransactionTest extends App {
    private val _2PL = 0
    private val TSO = 1
    private val numTrans = 10
    private val numOps   = 10
    private val numObjs  = 10
    
    //val t1 = new Transaction (new Schedule (List ( ('r', 0, 0), ('r', 0, 1), (w, 0, 0), (w, 0, 1) )),TSO)
    //val t2 = new Transaction (new Schedule (List ( ('r', 1, 0), ('r', 1, 1), (w, 1, 0), (w, 1, 1) )),TSO)


    //generate transactions

    val transactions = Array.ofDim[Transaction](numTrans)
    for (i <- transactions.indices) transactions(i) = new Transaction(Schedule.genSchedule2(i,numOps,numObjs),_2PL)
    VDB.initCache()
    for (i <- transactions.indices){
    	transactions(i).start()	
    } // for
    for (i <- transactions.indices){
    	transactions(i).join()
    } // for
    


} // TransactionTest object
