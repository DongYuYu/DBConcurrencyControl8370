
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
    private val tid         = nextCount ()        		    		// tarnsaction identifier
    private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set : [oid, (num_reads, num_writes)]
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
		begin()

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
        var lock        = LockTable.getObjLock( oid )			   // get the rrwl associated with this object from the lock table
        var writeLock   = lock.writeLock()					   // get the writeLock associated with the rrwl
	var noDeadlock  = checkForDeadlock(oid, lock, READ)			   // check to make sure this read request doesn't cause a deadlock
	var futureWrite = (rwSet contains oid) && rwSet(oid)(1) > 0
	
	if ( noDeadlock ){							   // if no deadlocks, try to lock
	
	   if ( writeLock.isHeldByCurrentThread() ) {				   // if you already own the writeLock so go ahead and read
	      println(s"$tid is reading because it already holds the write lock")
	   } // if
	   else if( futureWrite ) {							// else if you will want the write lock in the future 
	   	println(s"$tid wants to writeLock $oid for reading")
	   	noDeadlock = writeLockObj(oid,lock)
	   } // else							
	   else{									// else lock for regular reading
	   	println(s"$tid wants to readLock $oid")
	   	noDeadlock = readLockObj(oid,lock)
	   } // else
	
	   houseKeeping(oid,READ)
	   if( noDeadlock ) (VDB.read (tid,oid))._1
	   else {
	   	rollback()
		rec
	   } // else
	} // if 
	else {
	     rollback()
	     rec
	}
    } // read2PL
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write2PL (oid: Int, value: VDB.Record)
    {
	println(s"from trans: write($tid,$oid)")
        var lock      = LockTable.getObjLock(oid)
	var writeLock = lock.writeLock()
	var noDeadlock = checkForDeadlock(oid,lock,WRITE)
	if( noDeadlock ){
	    if(writeLock.isHeldByCurrentThread){					//if you own the writeLock, go ahead and write
	        println(s"$tid is writing $oid because it owns the writelock")
	    	VDB.write(tid, oid, value)
	    } 
            else{				    		   				//else try to writeLock the obj
	    	println(s"$tid wants to writeLock $oid")
		noDeadlock = writeLockObj(oid,lock)
		if( noDeadlock ) {
		    VDB.write(tid,oid,value)
		    println(s"$tid got to writeLock $oid")
		} // if
		else rollback()
	    }
	}
	else rollback()
        houseKeeping(oid,WRITE)
    } // write2PL    

    //::
    /**
     */
    def houseKeeping(oid: Int, readOrWrite: Int)
    {
	rwSet(oid)(readOrWrite) -= 1
	if( (rwSet(oid)(READ) == 0) && (rwSet(oid)(WRITE)==0) ) rwSet -= oid 
    }

    
    //:::::
    /**
     */
    def checkForDeadlock(oid: Int, lock: ReentrantReadWriteLock, readOrWrite: Int): Boolean =		// returns true if no deadlock detected, false otherwise
    {
	VDB.ch.synchronized{
		lock.synchronized{
			LockTable.table.synchronized{
				if( LockTable.getLockHolders(oid).size == 0 )          return true	// if nobody is locking this object you can't add a deadlock
				else if( lock.writeLock.isHeldByCurrentThread() )      return true	// if you hold the writeLock you can't add a deadlock
				else if(readOrWrite == READ && !lock.isWriteLocked() ) return true	// trying to read an object not writeLocked won't block
				else{		       	       			       	      		// otherwise you will block and may add a deadlock
					val waitGraph = cloneAndModifyPrecedenceGraph(oid)
					println(s"precedence graph from $tid")
					waitGraph.printG()
					if( hasCycle(waitGraph) ) return false
					else return true
				} // else
			} // synchronized
		}// synchronized
	}//synchronized
    } // checkForDeadLock()

    //::
    /**
     */
    def cloneAndModifyPrecedenceGraph(oid: Int): Graph =
    {
	val holder = LockTable.getLockHolders(oid)
	var edges = VDB.ch.clone
	//for( i <- VDB.ch.indices ) edges(i) = (VDB.ch(i).toSeq).toSet
	println(s"from cloneAndModify for $tid\nnumber of edges from clone: ${edges.size}\nholders:")
	for( trans <- holder ) println(s"$trans")
	for( trans <- holder ) edges(trans) += tid
	//val edges2 = Array(edges.toList)
	new Graph(edges)
    }
   
    //::
    /**
     */
    def writeLockObj(oid: Int, lock: ReentrantReadWriteLock): Boolean = 
    {
	val writeLock = lock.writeLock()
	//println(s"$tid wants to writeLock $oid")
	LockTable.table.synchronized{
		val owners = LockTable.getLockHolders(oid)
		if( owners.size == 0 ) {
		    LockTable.lock(oid, tid)
		    writeLock.lock()
		    writeLocks += (oid -> writeLock)
		    println(s"$tid got to writeLock $oid")
		    return true
		} // if
	} // synchronized
	var noDeadlock = checkForDeadlock(oid, lock, WRITE)				 // get here means owners != None
	if( noDeadlock ){
	    LockTable.table.synchronized{
		VDB.ch.synchronized{
			val owners = LockTable.getLockHolders(oid)
			/*if( owners.size != 0 )*/ for(i <- owners) VDB.ch(i) += tid
		}//synchronized
	    } // synchronized	
	    writeLock.lock()
	    noDeadlock = writeLockObj(oid, lock)
	} // if
	noDeadlock
	
    } // writeLockObj()
    

    def readLockObj(oid: Int, lock: ReentrantReadWriteLock): Boolean = 
    {
	val writeLocked = lock.isWriteLocked()
	val readLock    = lock.readLock()
	//println(s"$tid wants to readLock $oid")
	LockTable.table.synchronized{
		val owners = LockTable.getLockHolders(oid)
		if( owners.size == 0 || !writeLocked) {
		    LockTable.lock(oid, tid)
		    readLock.lock()
		    readLocks += (oid -> readLock)
		    println(s"$tid got to readLock $oid for reading")
		    return true
		} // if
	} // synchronized
	var noDeadlock = checkForDeadlock(oid, lock, READ)				 // get here means owners != None
	if( noDeadlock ){
	    LockTable.table.synchronized{
		VDB.ch.synchronized{
			val owners = LockTable.getLockHolders(oid)
			for(i <- owners) VDB.ch(i) += tid
		}//synchronized
	    } // synchronized	
	    readLock.lock()
	    noDeadlock = readLockObj(oid,lock)
	} // if
	noDeadlock
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
	val lock 	= LockTable.getObjLock(oid)
	val readLock 	= lock.readLock()
        if (writeTS <= tid){								// check if write_TS(X)<=TS(T), then we get to try to read  
		if(writeTS < tid){							// check for STSO, then use locks
			if( writeLocks contains oid ){					// read 'em if you got 'em
			    rec = VDB.read(tid,oid)._1
			    if(readTS < tid) VDB.tsTable(oid)(0) = tid
			} // if				
			else{								// THIS SHOULD HAPPEN
	    			readLock.lock()             // lock the readLock
                		LockTable.lock(oid,tid)
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
	     	 val lock      = LockTable.getObjLock(oid)
		 val writeLock = lock.writeLock()
		 if( writeLocks contains oid ) {				// SHOLDN'T HAPPEN write 'em if you got 'em
		     VDB.write(tid,oid,value)  					// writeLocks contains oid => we were last to write to this object => writeTS == tid
		     VDB.tsTable(oid)(WRITE) = tid				// SHOULD NEVER BE HERE
		 } // if		
		 else{								// i.e. - we don't have the lock yet
		 		 writeLock.lock()				// else lock before writing
                 		 LockTable.lock(oid,tid)
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
	for(oid <- readLocks.keys) {
	      println(s"releasing readLock for oid: ${oid}")
	      LockTable.unlock(oid,tid)
	      LockTable.checkLock(oid)
	      readLocks(oid).unlock()
	      readLocks -= oid
	} // for
    }


    //::
    /*
    **/
    def releaseWriteLocks()
    {
	println(s"releasing writeLocks for tid: $tid")
	for(oid <- writeLocks.keys) {
	      println(s"releasing writeLock for oid: ${oid}")
	      LockTable.unlock(oid,tid)
	      LockTable.checkLock(oid)
	      writeLocks(oid).unlock()
	      writeLocks -= oid    
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


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
  * > run-main trans.TransactionTest
  */

object TransactionTest extends App {
    private val _2PL = 0
    private val TSO = 1
    private val numTrans = 10
    private val numOps   = 10
    private val numObjs  = 50
    
    //val t1 = new Transaction (new Schedule (List ( ('r', 0, 0), ('r', 0, 1), (w, 0, 0), (w, 0, 1) )),TSO)
    //val t2 = new Transaction (new Schedule (List ( ('r', 1, 0), ('r', 1, 1), (w, 1, 0), (w, 1, 1) )),TSO)


    //generate transactions

    val transactions = Array.ofDim[Transaction](numTrans)
    for (i <- transactions.indices) transactions(i) = new Transaction(Schedule.genSchedule2(i,numOps,numObjs),_2PL)
    VDB.initCache()
    for (i <- transactions.indices){
    	transactions(i).start()
	transactions(i).join()
    } // for  


} // TransactionTest object
