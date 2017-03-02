/*
	TODO:
		find a thread safe collection to use
		implement the deadlock checker

		
		implement restart after roll back
		rigorously test roll back
		rigorously test 2PL and TSO implementations

	      
*/

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.2
 *  @date    Tue Jan 24 14:31:26 EST 2017
 *  @see     LICENSE (MIT style license file).
 *
 *
 */

package trans

import scala.collection.mutable.{ArrayBuffer, Map, Set}

import scala.util.control.Breaks._
import scala.util.Random
import java.io.{IOException, RandomAccessFile, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.HashMap
import Operation._

object TSTable
{

    private val DEBUG	   = false
    private val debugSynch = false
    
    /** Associative map of read time stamps. K => V :: (oid => read_TS)
     */
    private val readStamps = new HashMap [Int, Int]

    /** Associative map of write time stamps. K => V :: (oid +> write_TS)
     */
    private val writeStamps = new HashMap [Int, Int]

    /** Method to retrieve the read_TS for an object
     *  @param  oid the object to retrieve a read time stamp for. 
     */
    def readTS(oid: Int): Int =
    {
	readStamps.getOrElse(oid,-1)
    }
    
    /** Method to retrieve the write_TS for an object
     *  @param  oid the object to retrieve a write time stamp for. 
     */
    def writeTS(oid: Int): Int =
    {
	writeStamps.getOrElse(oid,-1)
    }

    /** 
     */
    def readStamp(tid: Int, oid: Int)
    {
	if(readStamps contains oid) readStamps(oid) = tid
	else readStamps += ((oid, tid))
    }

    def writeStamp(tid: Int, oid: Int)
    {
	if(writeStamps contains oid) writeStamps(oid) = tid
	else writeStamps += ((oid, tid))
    }
}

object WaitsForGraph
{

    private val DEBUG      = false
    private val debugSynch = false
    
    var graph = new Graph()

    def addEdge(u: Int, v: Int)    = graph.addEdge(u,v)
    
    def removeEdge(u: Int, v: Int) = graph.removeEdge(u,v)

    def addNode(u: Int)       	   = graph.addNode(u)

    def removeNode(u: Int) 	   = graph.removeNode(u)

    def printG()   		   = graph.printG2() 
    
    def checkForDeadlocks(tid: Int, oid: Int, lock: ReentrantReadWriteLock, readOrWrite: Int) : Boolean =
    {
    	val READ = 0
	val WRITE = 1
	var noDeadLock = false
	var req = ""
	if(readOrWrite == READ) req = "readLock" else req = "writeLock"
	if(debugSynch) println(s"$tid entering WaitsForGraph synch block in ck4DeadLocks")
	synchronized{
		if(debugSynch) println(s"$tid entered WaitsForGraph synch block in ck4DeadLock")
		val writeLocked = lock.isWriteLocked()
		val noReaders   = lock.getReadLockCount() == 0
		val noOwners    = !writeLocked && noReaders
		if( noOwners ){	    
		    if( DEBUG ) println(s"In Ck4DeadLock found an open object in $oid")
		    noDeadLock = true
		} // if		
		else if( lock.writeLock.isHeldByCurrentThread() ){
		    if( DEBUG ) println(s"In Ck4DeadLock Found that $oid was already writeLocked by $tid")
		    noDeadLock = true
		} // else if	
		else if( readOrWrite == READ && !lock.isWriteLocked() ){
		     if( DEBUG ) println(s"In Ck4DeadLock Found that $oid is shared locked and $tid wants to share the lock.")
		     noDeadLock = true
		} // ese if 
		else {
		     if( DEBUG ) {
		     	 println(s"Cking deadlocks for $tid request to $req $oid. Current waits for graph: ")
		     	 printG()
		    	 println("")
		     }
		     for( owner <- LockTable.getOwners(oid) ) graph.addEdge(tid, owner)
		     //println(s"edges added temporarilly to evaluate $req request from $tid to lock $oid")
		     if( graph.hasCycle() ){
         	     	 for( owner <- LockTable.getOwners(oid) ) graph.removeEdge(tid, owner)
			 if(DEBUG)println(s"${tid}'s request to $req $oid DENIED ")
		     }
		     else if( DEBUG ){
		     	  println(s"${tid}'s request to $req $oid GRANTED. Current waits for graph: ")
			  noDeadLock = true
		     }
		} // else
	} // synchronized
	if(debugSynch) println(s"$tid exited WaitsForGraph synch block in ck4DeadLock")
	noDeadLock
    }
}

object ScheduleTracker
{
    private val sched = ArrayBuffer[Op]()

    def addOp(op: Op){
    	synchronized{
	    sched += op
	} // synch
    } // addOp

    def getSchedule(): Array[Op] =
    {
	synchronized{
	    sched.toArray	
	} // synch

    } // getSched

    def purgeTransaction(tid: Int)
    {
	synchronized{
	    sched --= sched.filter(op => op._2 == tid)
	} // synch
    }
}

object LockTable
{

    private val DEBUG	   = false
    private val debugSynch = false

    /** Associative map of locks associated with objects K => V :: (oid => lock)
     */
    private var locks = new HashMap [Int, ReentrantReadWriteLock] ()

    /** An associate map of locks to the owners of the locks: K => V :: (oid => set of owners )
     */
    private var owners = new HashMap [Int, scala.collection.mutable.Set[Int]] ()

    /** 
     */
    private var waiters = new HashMap [Int, scala.collection.mutable.Set[Int] ] ()

    /** Retrieve the lock associated with an object
     */
    def getObjLock(oid: Int) : ReentrantReadWriteLock = {
    	var lock = new ReentrantReadWriteLock()
	if(debugSynch) println(s"entering Lockable synch block in getObjLock")
    	synchronized{
		if(debugSynch) println(s"entered Lockable synch block in getObjLock")
		if ( locks contains oid ) lock = locks(oid)
		else{
			locks += ((oid,lock))
		} // else
	} // synchronized
	if(debugSynch) println(s"exited LockTable synch block in getObjLock")
	lock
    }

    def getOwners(oid: Int): scala.collection.mutable.Set[Int] =
    {
	//println("entering synchronized block to getOwners")
	var ret = scala.collection.mutable.Set[Int] ()
	if(debugSynch) println(s"entering Lockable synch block in getOwners")
	synchronized{
		if(debugSynch) println(s"entered LockTable synch block in getOwners")
		if( owners contains oid ) ret = owners(oid)
	}
	if(debugSynch) println(s"exited LockTable synch block in getOwners")
	ret
    }

    def addOwner(oid: Int, tid: Int)
    {
	if(debugSynch) println(s"$tid entering Locktable synch block in addOwner")
	synchronized{
		if(debugSynch) println(s"$tid entered Lockable synch block in addOwner")
		if(owners contains oid) owners(oid) += tid
		else {
		     //println(s"$tid adding new owner entry in LockTable for $oid.")
		     owners(oid) = scala.collection.mutable.Set(tid)
		     //println(s"$owners(oid)")
		}
	}
	if(debugSynch) println(s"$tid exited Lockable synch block in addOwner")
	//println("left synchronized block to addOwner")
    }

    //::
    /**/
    def addWaiter(oid: Int, tid: Int)
    {
    	if(debugSynch) println(s"$tid entering Locktable synch block in addWaiter")
	synchronized{
	    if(debugSynch) println(s"$tid entered Locktable synch block in addWaiter")
	    if( waiters contains oid) waiters(oid) += tid
	    else waiters(oid) = scala.collection.mutable.Set(tid)
	} // synch
	if(debugSynch) println(s"$tid exited Locktable synch block in addWaiter")
    }

    //::
    /**/
    def removeWaiter(oid: Int, tid: Int)
    {
	if(debugSynch) println(s"$tid entering Locktable synch block in removeWaiter")
	synchronized{
	    if(debugSynch) println(s"$tid entered Locktable synch block in removeWaiter")
	    if( waiters contains tid) waiters -= oid
	} // synch
	if(debugSynch) println(s"$tid exited Locktable synch block in removeWaiter")
    }

    //::
    /**/
    def getWaiters(oid: Int): scala.collection.mutable.Set[Int] =
    {
	var ret = scala.collection.mutable.Set[Int]()	    	
        if(debugSynch) println(s"entering Locktable synch block in getWaiters")
	synchronized{
		if(debugSynch) println(s"entered Locktable synch block in getWaiters")
		if(waiters contains oid) ret = waiters(oid).clone
	} // synch
	if(debugSynch) println(s"exited Locktable synch block in getWaiters")
	ret
    }

    //::
    /**/
    def updateWaiters(oid: Int, tid: Int)
    {
	val v = tid
	if(debugSynch) println(s"$tid entering Locktable synch block in updateWaiters")
	synchronized{
		if(debugSynch) println(s"$tid entered Locktable synch block in updateWaiters")
		if(waiters contains oid){
	    		   for(u <- waiters(oid) ) {
			   	 //println(s"updating wait graph with edge $u -> $v")
			   	 WaitsForGraph.addEdge(u,v)
			   }
		} // if
	} // synch
	if(debugSynch) println(s"$tid exited Locktable synch block in updateWaiters")
    } // updateWaiters
    
    /*******************************************************************************
     * Unlock/release the lock on data object oid.
     * @param tid  the transaction id
     * @param oid  the data object id
     */
    def unlock (tid: Int, oid: Int)
    {
	if( (owners contains oid) && (owners(oid) contains tid) ) {
	    owners(oid) -= tid
	    //println(s"owners of lock for $oid: ${owners(oid).mkString}")
	    if( owners(oid).size == 0 ) owners = owners - oid
	} // if
	else if( !(owners contains oid) && DEBUG) println(s"$tid tried to unlock object $oid which didn't have any owners")
	else if( DEBUG )println(s"$tid tried to unlock object $oid that it didn't own.")
    } // ul

    /*******************************************************************************
     * Convert the lock table to a string.
     */
    override def toString: String =
    {
	var ret = "LockTable: \n"
    	//println("entering synchronized block to toString")
        synchronized {
	    //println("entered synchronized block to toString")
	    for(oid <- locks.keys){
	    	    ret += (oid + ": ")
		    ret += owners.get(oid).mkString(" ")
		    ret += "\n"		    
	    }
	}
	//println("left synchronized block to toString")
	ret
    } // toString
}


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDB` object represents the Volatile Database.
 */
object VDB
{
    type Record = Array [Byte]                           // record type
    type LogRec = Tuple4 [Int, Int, Record, Record]      // log record type (tid, oid, v_old, v_new)
    
    private val DEBUG         = false                    // debug flag
    private val debugSynch    = false
    private val CSR_TESTING   = true 

    private val pages         = 5                        // number of pages in cache
    private val recs_per_page = 32                       // number of record per page
    private val record_size   = 128                      // size of record in bytes
    private val log_rec_size  = 264			 // size of a log record

    private val BEGIN    = -1
    private val COMMIT   = -2
    private val ROLLBACK = -3

    private var lastCommit = -1
    var numWrites = 0	

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** The `Page` case class 
     */
    case class Page ()
    {
         val p = Array.ofDim [Record] (recs_per_page)
         override def toString = s"Page( + ${p.deep} + )\n" 
    } // page class

                    val cache		 = Array.ofDim [Page]     (pages)	// database cache
                    val logBuf 		 = ArrayBuffer [LogRec]   ()            // log buffer
	    private val map    		 = Map         [Int, Int] ()            // map for finding pages in cache (pageNumber -> cpi)
	    	    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Initialize the cache.
     */
    def initCache ()
    {
        for (i <- 0 until pages) {
            val pg = Page ()
            for (j <- 0 until recs_per_page) pg.p(j) = genRecord (i, j)
            cache(i) = pg
            map += i -> i 
        } // for
    } // initCache
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid' from the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def read (tid: Int, oid: Int): (Record, Int) =
    {
		synchronized{
		    if (DEBUG) println (s"read ($tid, $oid)")
		    val op = (r,tid,oid)
		    if (CSR_TESTING) ScheduleTracker.addOp(op)
		    val pageNum = oid/recs_per_page
		    var cpi = 0
		    var pg = new Page()
		    var rec: Record = null
		    if(map contains (pageNum)){				// is the page in the cache already? 
			cpi = map(pageNum)         			// the cache page index
			pg = cache(cpi)                        		// page in cache
			rec = pg.p(oid % recs_per_page)			//record location in cache page
			return (rec,cpi)
		    } // if
		    else							// the page is not in the cache 
		    {
			return cachePull(oid)
		    } // else
		    //(rec, cpi)
		} // synch
    } // read


    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid' from the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def reread (tid: Int, oid: Int): (Record, Int) =
    {
		synchronized{
		    if (DEBUG) println (s"reread ($tid, $oid)")
		    val op = (r,tid,oid)
		    //if (CSR_TESTING) ScheduleTracker.addOp(op)
		    val pageNum = oid/recs_per_page
		    var cpi = 0
		    var pg = new Page()
		    var rec: Record = null
		    if(map contains (pageNum)){				// is the page in the cache already? 

			cpi = map(pageNum)         			// the cache page index
			pg = cache(cpi)                        		// page in cache
			rec = pg.p(oid % recs_per_page)			//record location in cache page
			return (rec,cpi)
		    } // if
		    else							// the page is not in the cache 
		    {
			return cachePull(oid)
		    } // else
		    //(rec, cpi)
		} // synch
    } // read

	//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/**  A method to pull a page from the PDB into the cache
	  * @param pageNumber  the number of the page in the PDB we wish to pull into the cache
	  *  @return (record associated with oid, cachePage to fill with PDB page containing record for oid)
	  */
	def cachePull(oid : Int) : (Record, Int) =
	{
		val newPageNumber = oid/recs_per_page
		val newPage = PDB.fetchPage(newPageNumber)
		val victim = victimize()
		val (victimPageNum, cpi) = victim
		if (victimPageNum>=0) map -= victimPageNum
		map += (newPageNumber -> cpi )
		cache(cpi)=newPage
		(newPage.p(oid % recs_per_page),cpi)			//record location in cache page
	}

	//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** A method to chose a cache page victim.
 	  */

	def victimize() : (Int, Int) =
	{

		if (map.nonEmpty){
			val rand = Random.nextInt(map.size)				// the random victim
			val keys = map.keys.toSeq					// a sequence containing the page numbers in the cache 
			var k = keys(rand)						// the randomly selected cache page number
			val v = map.getOrElse(k,0)					// the cpi for the randomly selected page number from the cache
			PDB.write(k,cache(v))						// PDB.write(page number, page value)
			(k,v)

		}
		else{
			var free = 0
			if( map.keys.size > 0 ){
			    breakable{	
			    	for( i <- cache.indices ){
				     if(cache(i) == None ) {
				     		 free = i
						 break
				     } // if
			    	} // for
			    } // breakable
			} // if
			(-1,free)						// non-full cache return value
		}
	}
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the 'newVal' record to the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def write (tid: Int, oid: Int, newVal: Record)
    {
	synchronized{
	numWrites+=1
	if (DEBUG) println (s"write ($tid, $oid, $newVal)")
	val op = (w,tid,oid)
	if (CSR_TESTING) ScheduleTracker.addOp(op)

	if (newVal == null) println(s"Cannot write null values to the database.")
	else{
		val (oldVal, cpi) = read (tid, oid)			//get the old value and it's cpi from read
		val recOffset 	  = oid % recs_per_page			
		val pageNumber 	  = oid / recs_per_page
		
		//if(DEBUG) println("old logBuf.size: " + logBuf.size)
		
	        logBuf += ((tid, oid, oldVal, newVal))			//add the operation to the logBuf

		//if(DEBUG) println("new logBuf.size: " + logBuf.size)
		
	        val pg		= cache(map(pageNumber))	 	//Note: data value should be cached by read 
	        pg.p(recOffset) = newVal				//change the old value in the page to the new value
	}
<<<<<<< HEAD
	} // synch
    } // write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the 'newVal' record to the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def rewrite (tid: Int, oid: Int, newVal: Record)
    {
	synchronized{
	numWrites-=1
	if (DEBUG) println (s"rewrite ($tid, $oid, $newVal)")
	val op = (w,tid,oid)
	//if (CSR_TESTING) ScheduleTracker.addOp(op)					//don't record in the schedule tracker
	if (newVal == null) println(s"Cannot write null values to the database.")
	else{
		val (oldVal, cpi) = reread (tid, oid)			//get the old value and it's cpi from read
		val recOffset 	  = oid % recs_per_page			
		val pageNumber 	  = oid / recs_per_page
		
		//if(DEBUG) println("old logBuf.size: " + logBuf.size)
		
	        //logBuf += ((tid, oid, oldVal, newVal))			// redo's don't go into the log, do they? 

		//if(DEBUG) println("new logBuf.size: " + logBuf.size)
		
	        val pg		= cache(map(pageNumber))	 	//Note: data value should be cached by read 
	        pg.p(recOffset) = newVal				//change the old value in the page to the new value
	}
	} // synch
    } // write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def begin (tid: Int)
    {	
    	//println(s"transaction $tid entering synchronized block to begin")
	synchronized{
	    //println(s"transaction $tid entered synchronized block to begin")
	    if (DEBUG) println (s"begin ($tid)")
            logBuf += ((tid, BEGIN, null, null))
	    WaitsForGraph.addNode(tid)
	    //println(s"Added transaction $tid. Graph: ")
	    //WaitsForGraph.printG()
	    //println("")
	}
	//println(s"transaction $tid left synchronized block to begin")	>>>>>>> 3d9ab41adbc5c5ff0ddaccb6ea608289968c9786
    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def commit (tid: Int)
    {
	//println(s"transaction $tid entering synchronized block to commit")
	if(debugSynch) println(s"$tid entering VDB synchronized block")
	synchronized{
		if(DEBUG) println(s"commit($tid)")
		if(debugSynch) println(s"$tid entered VDB synchronized block")
		logBuf += ((tid, COMMIT, null, null))
        	if (DEBUG) {
	   	   println (s"commit ($tid)")
	   	   //printLogBuf()
		}
		flushLogBuf()			 				//flush the logBuf
		lastCommit = logBuf.length - 1					//update the lastCommit pointer
		//println(s"$tid committed from VDB")
		//if( DEBUG ) print_log()
	} // synch
	if(debugSynch) println(s"$tid exited VDB synchronized block")
    } // commit
    
    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Method to flush the logBuf contents into the log_file. 
     */
    def flushLogBuf() {
		var raf = new RandomAccessFile(PDB.log_file, "rw")
		for (i <- lastCommit + 1 to logBuf.length - 1) {
			var bb = ByteBuffer.allocate(264)
			//get a new ByteBuffer
			var data = logBuf(i)                //grab the current record to flush
			bb.putInt(data._1)
			bb.putInt(data._2)
			if (data._3 != null) bb.put(data._3)        //can't put(null) values
			else bb.put(("-" * 128).getBytes())
			if (data._4 != null) bb.put(data._4)            //again, null
			else bb.put(("-" * 128).getBytes())
			var ba = bb.array()
			raf.seek(raf.length())                //make sure to be appending
			raf.write(ba)
			//println("FlushlogBuff")
		} // for
	} // flushLogBuf()
    
    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Print out the contents of the log buffer. 
     */
    def printLogBuf() {
    	for(i <- logBuf.indices ) println(s"logBuf @ $i: ${logBuf(i)}")
    }
    
    
    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Print the current contents of the log_file
     *  @param raf  The log_file
     */
    def print_log() 
    {
	var raf = new RandomAccessFile(PDB.log_file,"rw")
     	raf.seek(0)						
     	//var buf = Array.ofDim[Byte](log_rec_size) ??
	var buf = Array.ofDim[Byte](128)
     	var count = 0;
     	var read = raf.read(buf)
     	print(s"read: $read")
     	while( read != -1 ){
     	    println(s"count: $count")
     	    var bb = ByteBuffer.allocate(264)

	    bb.put(buf);

	    bb.position(0)
	    println(s"(${bb.getInt()},${bb.getInt()},"        +
	       	       s"${bb.array.slice(8,135).toString()},"  +
		       s"${bb.array.slice(136,263).toString()}")
	    read = raf.read(buf)
	    count+=1
	}// while
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def rollback (tid: Int)
    {
	synchronized{
		if (DEBUG) println (s"rollback ($tid)")
        	logBuf += ((tid, ROLLBACK, null, null))
		var i = logBuf.length-2
		var rolling = true
		var data = Tuple4(0,0,Array.ofDim[Byte](record_size) ,Array.ofDim[Byte](record_size))
		while(rolling && i >= 0){
		    val (rec_tid, oid, oldVal, newVal) = logBuf(i)
		    if( rec_tid == tid ){
		    	if( oid != BEGIN ){
		    	    val page       = oid/32
		    	    if(map contains page){
		    	    rewrite(tid, oid, oldVal);
		    	} // if
		    	else{
			   val (rec,cpi) = cachePull(page)
			   rewrite(tid,oid,oldVal)
		    	} // else
		    } // if
		    else rolling = false
		    } // else
		    i-=1
		}// while
        	WaitsForGraph.removeNode(tid)
		ScheduleTracker.purgeTransaction(tid)
    	} // synch
    } // rollback
    
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Generate the (i*32+j)th record.
     *  @param i  the page number
     *  @param j  the record number within the page
     */
    def genRecord (i: Int, j: Int): Record = str2record (s"Page $i Record $j ")

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Convert a string to a record.
     *  @param str  the string to convert 
     */
    def str2record (str: String): Record = (str + "-" * (record_size - str.size)).getBytes

} // VDB

object PDB
{


	val log_file   = "log"
	val store_file = "store"
	private val pages = 15
	private val recs_per_page = 32
	private val record_size = 128
	val store = new RandomAccessFile(store_file,"rw")
	val log = new RandomAccessFile(log_file,"rw")

	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** The `Page` case class
	  */
	case class Page ()
	{
		val p = Array.ofDim [VDB.Record] (recs_per_page)
		override def toString = s"Page( + ${p.deep} + )\n"
	} // page class
	
	def write (pageNum:Int, page: VDB.Page)
	{
		store.seek(pageNum*recs_per_page*record_size)
		var p = page.p
		for (i <- p.indices) {                //make sure to be appending
			store.write(p(i))
		} // for

	}
	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Initialize the store
	  */
	def initStore()
	{
		for (i <- 0 until pages) {
			val pg = Page()
			for (j <- 0 until recs_per_page) pg.p(j) = genRecord(i, j)
			store.write(toByteArray(pg))
		} // for

	}
	def toByteArray (page: Page): Array[Byte]={
		page.p.flatMap(_.map((b:Byte)=>b))
	}

	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Generate the (i*32+j)th record.
	  *  @param i  the page number
	  *  @param j  the record number within the page
	  */
	def genRecord (i: Int, j: Int): VDB.Record = str2record (s"Page $i Record $j ")

	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Convert a string to a record.
	  *  @param str  the string to convert
	  */
	def str2record (str: String): VDB.Record = (str + "-" * (record_size - str.size)).getBytes



	def recover
	{
	}
	
	//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/**  A method to read a page from the PDB and return the content of the page
	  * @param page  the number of the page in the PDB we wish to pull into the cache
	  *  @return VDB.Page the content of the page in the PDB
	  */
	def fetchPage(page: Int): VDB.Page =
	{
		//println("reading from store")
		//println(s"size of store: ${store.length()}")
		var buf = Array.ofDim[Byte](record_size)
		store.seek(page * recs_per_page * record_size)
		var pg = VDB.Page()
		var p = pg.p
		for(i <- p.indices){
		      	p(i) = Array.ofDim[Byte](record_size)
			//println(s"p(i).length: ${p(i).length}")
			var bb = ByteBuffer.allocate(record_size)
			var read = store.read(buf)
			//println(s"read $read many bytes from the store")
			bb.put(buf)
			//println(s"arrayOffset: ${bb.arrayOffset()}")
			//println(s"bb.array.length: ${bb.array.length}")
			bb.position(0)
			bb.get(p(i))
		} // for
		pg
	}
}

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDBTest` object is used to test the `VDB` object.
 *  > run-main trans.VDBTest
 */
object VDBTest extends App
{
    PDB.initStore()
    VDB.initCache ()
    println ("\nPrint cache")
    for (pg <- VDB.cache; rec <- pg.p) println (new String (rec))   // as text
//  for (pg <- VDB.cache; rec <- pg.p) println (rec.deep)           // as byte array
//  for (pg <- VDB.cache; rec <- pg.p) println (rec.size)           // number of bytes

    println ("\nTest reads and writes:")
    println ("reading1 read (2, 40)")
    println (new String (VDB.read (2, 40)._1))
    val newVal = VDB.str2record ("new value for record 40 ")
    println (s"write (2, 40, ${new String (newVal)})")
    VDB.write (2, 40, newVal)
    println ("reading2 read (2, 40)")
    println (new String (VDB.read (2, 40)._1))
    
    println ("\nPrint cache")
    for (pg <- VDB.cache; rec <- pg.p) println (new String (rec))   // as text


    println("logBuf size: " + VDB.logBuf.size)
    println ("\nPrint logBuf")
    for (i <- VDB.logBuf.indices) println(VDB.logBuf(i))
    
    VDB.commit(2);
    PDB.fetchPage(2)
} // VDBTest

object VDBTest2 extends App
{
 	val OPS_PER_TRANSACTION  = 10
	val TOTAL_TRANSACTIONS   = 50
    	val TOTAL_OBJECTS	 = 480
	val TOTAL_OPS 		 = OPS_PER_TRANSACTION * TOTAL_TRANSACTIONS

	val _2PL = 0
	val TSO  = 1
	PDB.initStore()
   	VDB.initCache ()

	var transactions = Array.ofDim[Transaction](TOTAL_TRANSACTIONS)
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i) = new Transaction( Schedule.genSchedule2(i,OPS_PER_TRANSACTION, TOTAL_OBJECTS) , _2PL)
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i).start()
	println("all transactions started")
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i).join()
	Thread.sleep(12000) 
	println("::////////////////////////////////\nall transactions finished\n\n\n\n")
	println(s"Schedule length correct : ${ScheduleTracker.getSchedule().toList.length == TOTAL_OPS+VDB.numWrites}")
    	val schedule = new Schedule( ScheduleTracker.getSchedule().toList )
	println(s"$schedule")
    	println(s"Resulting schedule is CSR: ${schedule.isCSR(Transaction.nextCount())}")
 
	System.exit(0)
} // VDBTest2B
