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

import scala.collection.mutable.{ArrayBuffer, Map}

import scala.util.control.Breaks._
import scala.util.Random
import java.io.{IOException, RandomAccessFile, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

import Operation._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The 'LockTable' object represents the lock table for the VDB
 */
 object LockTable
 {

	private val table = Map[Int, ReentrantReadWriteLock] ()			// Map used to access locks for each object
		    	    	     			     			// (oid => lock)
	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Method to retrieve a WriteLock for this object. Creates a lock
	 *  if there is not a lock associated with this object in the lock
	 *  table.
	 *  @ param tid  The integer id for the transaction trying to lock the object
	 *  @ param oid  The integer id for the object the transaction is trying to lock
	 */

	def lock(oid: Int): ReentrantReadWriteLock = 
	{
		this.synchronized{
			var lock = table.get(oid)					// Retrieve the RRWL associated with the object
			if( lock == None ){						// in the lock table
				table += (oid -> new ReentrantReadWriteLock(true))		// with the object in the table
				lock = table.get(oid)

			}// if
			lock.get							// Return the WriteLock for this RRWL

		}

	} // lock

	
	//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Method to remove unnecessary locks from the table. Check the waiters
	 *  for the lock, if none remove from the table.
	 *  @ param oid  The object identifier the lock is associated with.
	 */
	def checkLock(oid: Int) 
	{
		this.synchronized {
		  if(table contains oid){
		  	   var lock = table(oid)
      		  	   if( !(lock.hasQueuedThreads()) ) table -= oid	// take the lock out of the table, since no one wants it
		  }//if 
		}
	}
 }

class adjustLock extends ReentrantReadWriteLock {
	override def getOwner: Thread = super.getOwner


}


//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDB` object represents the Volatile Database.
 */
object VDB
{
    type Record = Array [Byte]                           // record type
    type LogRec = Tuple4 [Int, Int, Record, Record]      // log record type (tid, oid, v_old, v_new)

    private val DEBUG         = true                     // debug flag
    private val CSR_TESTING   = false 
    private val pages         = 5                        // number of pages in cache
    private val recs_per_page = 32                       // number of record per page
    private val record_size   = 128                      // size of record in bytes
    private val log_rec_size  = 264			 // size of a log record


	import scalation.graphalytics.Graph
	import scala.collection.immutable.Set
	import scalation.graphalytics.Cycle.hasCycle


    
    var tsTable = Array.ofDim[Int](pages*recs_per_page, 2)  //create a Timestamp Table record the (rts, ws) of oid
    private val BEGIN    = -1
    private val COMMIT   = -2
    private val ROLLBACK = -3

    private var lastCommit = -1

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
	    	    val SCHEDULE_TRACKER = ArrayBuffer [Op]       ()
		    val sched_file       = new RandomAccessFile("sched_file","rw")
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
		if (DEBUG) println (s"read ($tid, $oid)")
		val op = (r,tid,oid)
		
		if (CSR_TESTING) {
	  	   	SCHEDULE_TRACKER.synchronized{
				SCHEDULE_TRACKER += op
			}// synchronized
	  	   	val sched_file = new RandomAccessFile("sched_file","rw")
		   	   
			var bb = ByteBuffer.allocate(12)
	  	 	//get a new ByteBuffer
	   		bb.putInt(0)
	   		bb.putInt(tid)
	   		bb.putInt(oid)
	   		var ba = bb.array()
	   		sched_file.seek(sched_file.length())                //make sure to be appending
	   		sched_file.write(ba)
	   		//println("FlushlogBuff")
	   		//println(s"SCHEDULE_TRACKER from write method: ${SCHEDULE_TRACKER}")
		} // if
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
			val num = Random.nextInt(map.size)
			val key = map.keys.toSeq
			var num1 = key(num)
			val v = map.getOrElse(num1,0)
			val elem = (num1, v)
			//val elem = map.last
			PDB.write(elem._1,cache(elem._2))
			elem
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
        if (DEBUG) println (s"write ($tid, $oid, $newVal)")
	val op = (w,tid,oid)
	if (CSR_TESTING) {
	   SCHEDULE_TRACKER += op
	   val sched_file = new RandomAccessFile("sched_file","rw")
	   var bb = ByteBuffer.allocate(12)
	   //get a new ByteBuffer
	   bb.putInt(1)
	   bb.putInt(tid)
	   bb.putInt(oid)
	   var ba = bb.array()
	   sched_file.seek(sched_file.length())                //make sure to be appending
	   sched_file.write(ba)
	   //println("FlushlogBuff")
	   //println(s"SCHEDULE_TRACKER from write method: ${SCHEDULE_TRACKER}")
	} // if

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
        
    } // write

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Begin the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def begin (tid: Int)
    {
        if (DEBUG) println (s"begin ($tid)")
        logBuf += ((tid, BEGIN, null, null))

    } // begin

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Commit the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def commit (tid: Int)
    {
 	logBuf += ((tid, COMMIT, null, null))
        if (DEBUG) {
	   println (s"commit ($tid)")
	   //printLogBuf()
	}

	flushLogBuf()			 				//flush the logBuf
	
	lastCommit = logBuf.length - 1					//update the lastCommit pointer

	//if( DEBUG ) print_log()
	
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
		    		  write(tid, oid, oldVal);
		    	}// if
		    	else{
				val (rec,cpi) = cachePull(page)
				write(tid,oid,oldVal)
			}// else
		    }// if
		    else rolling = false
		}// if
		i-=1
	}// while
        // I M P L E M E N T
    } // rollback

    def mydefault0 = -1

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

	try{
		val store = new RandomAccessFile(store_file,"rw")
		val log = new RandomAccessFile(log_file,"rw")
	}
	catch{
		case iae  : IllegalArgumentException => println("IllegalArgumentException: " + iae)
		case fnfe : FileNotFoundException    => println("FileNotFOundException in PDB: " + fnfe)
		case se   : SecurityException        => println("SecurityException: " + se)
	}

	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** The `Page` case class
	  */
	case class Page ()
	{
		val p = Array.ofDim [VDB.Record] (recs_per_page)
		override def toString = s"Page( + ${p.deep} + )\n"
	} // page class
	def write (pageNum:Int, page: VDB.Page) ={
		var store = new RandomAccessFile(PDB.store_file,"rw")
		store.seek(pageNum*recs_per_page*record_size)
		var p = page.p
		for (i <- p.indices) {                //make sure to be appending
			store.write(p(i))
		} // for

	}
	//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Initialize the store
	  */
	def initStore() ={
		val store = new RandomAccessFile(store_file,"rw")
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
		val store = new RandomAccessFile(PDB.store_file,"rw")
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

} // VDBTest

object VDBTest2 extends App
{
 	val OPS_PER_TRANSACTION  = 15
    	val TOTAL_TRANSACTIONS   = 5
    	val TOTAL_OBJECTS	 = 480

	PDB.initStore()
   	VDB.initCache ()

	var transactions = Array.ofDim[Transaction](TOTAL_TRANSACTIONS)
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i) = new Transaction( Schedule.genSchedule2(i,OPS_PER_TRANSACTION, TOTAL_OBJECTS) )
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i).start()
	println("all transactions started")
	for( i <- 0 until TOTAL_TRANSACTIONS) transactions(i).join()
	println("all transactions finished")
	
	//println(s"\n\n\n\n********FINAL SCHEDULE_TRACKER*************************")
	//println(s"\n\n\n\n********FINAL SCHEDULE_TRACKER: ${VDB.SCHEDULE_TRACKER}*************************")
	//var raf = new RandomAccessFile("sched_file","rw")

	val sched = new Schedule( VDB.SCHEDULE_TRACKER.toList )
	println(s"This was a CSR Schedule: ${sched.isCSR(TOTAL_TRANSACTIONS)}") 
	System.exit(0)
} // VDBTest2

object VDBTest2_2 extends App
{
     val TOTAL_TRANSACTIONS   = 5
     var raf = new RandomAccessFile("sched_file","rw")
     raf.seek(0)						
     var buf   = Array.ofDim[Byte](12)
     var read  = raf.read(buf)
     var s = ArrayBuffer[(Char,Int,Int)]()
     while( read != -1 ){
     	    println(s"read: $read")
     	    //println(s"count: $count")
     	    var bb = ByteBuffer.allocate(12)
	    bb.put(buf);
	    bb.position(0)
	    var firstNum = bb.getInt()
	    var firstLet = r
	    if( firstNum == 0 ) firstLet = r
	    else firstLet = w
	    val tup = (firstLet,bb.getInt(),bb.getInt())
	    s += tup
	    read = raf.read(buf)
     }// while
     val sched = new Schedule( s.toList )
     println(sched)
     println(s"This was a CSR Schedule: ${sched.isCSR(TOTAL_TRANSACTIONS)}") 
} // VDBTest2_2

    