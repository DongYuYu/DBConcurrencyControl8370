/*
	TODO: Implement disk reads
	      Restart after roll back
	      Initialize the PDB
	      
*/

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.2
 *  @date    Tue Jan 24 14:31:26 EST 2017
 *  @see     LICENSE (MIT style license file).
 *
 *	I'm adding these for Medhi right now. 
 */

package trans

import scala.collection.mutable.{ArrayBuffer, Map}
import java.io.{IOException, RandomAccessFile, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
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
		var lock = table.get(oid)					// Retrieve the RRWL associated with the object
		if( lock == None ){						// in the lock table
		    table += (oid -> new ReentrantReadWriteLock(true))		// with the object in the table
		    lock = table.get(oid)
		}// if
       	        lock.get							// Return the WriteLock for this RRWL
	} // lock

	
	//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
	/** Method to remove unnecessary locks from the table. Check the waiters
	 *  for the lock, if none remove from the table.
	 *  @ param oid  The object identifier the lock is associated with.
	 */
	def checkLock(oid: Int) 
	{
		var lock = table(oid)
		if( lock != None && !(lock.hasQueuedThreads()) ) table -= oid	// take the lock out of the table, since no one wants it
	}

	
	
 }
 
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDB` object represents the Volatile Database.
 */
object VDB
{
    type Record = Array [Byte]                           // record type
    type LogRec = Tuple4 [Int, Int, Record, Record]      // log record type (tid, oid, v_old, v_new)

    private val DEBUG         = true                     // debug flag
    private val ANALYZE       = true
    private val pages         = 5                        // number of pages in cache
    private val recs_per_page = 32                       // number of record per page
    private val record_size   = 128                      // size of record in bytes

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

            val cache  = Array.ofDim [Page] (pages)      // database cache
            val logBuf = ArrayBuffer [LogRec] ()         // log buffer
    private val map    = Map [Int, Int] ()               // map for finding pages in cache

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
	if (ANALYZE) logBuf += ((tid, oid, null, null))
	val pageNum: Int = oid/recs_per_page
        if(map contains (pageNum){
		val cpi = map(oid / recs_per_page)         // the cache page index
		val pg = cache(cpi)                        // page in cache
	        (pg.p(oid % recs_per_page), cpi)           // record, location in cache
	} // if
	else
	{
		///////////////////////////////////////////// TODO: Consider the cpi = NULL, i.e. - need to do a disk read...
	} // else
	
        
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the 'newVal' record to the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def write (tid: Int, oid: Int, newVal: Record)
    {
        if (DEBUG) println (s"write ($tid, $oid, $newVal)")
	if (newVal == null) println(s"Cannot write null values to the database.")
	else{
		val (oldVal, cpage) = read (tid, oid)
		/////////////////////////////////////////// what if we didn't have the value in the cache? Is it handled in the read? I think so...
		println("old logBuf.size: " + logBuf.size)
	        logBuf += ((tid, oid, oldVal, newVal))
		println("new logBuf.size: " + logBuf.size)
	        val pg = cache(map(oid / recs_per_page))		//Note: data value should be cached by read 
	        pg.p(oid % recs_per_page) = newVal
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
	   printLogBuf()
	}

	flushLogBuf()			 				//flush the logBuf
	
	lastCommit = logBuf.length - 1					//update the lastCommit pointer

	if( DEBUG ) print_log()
	
    } // commit

    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Method to flush the logBuf contents into the log_file. 
     */
    def flushLogBuf()
    {
	var raf = new RandomAccessFile(PDB.log_file,"rw")	
	for( i <- lastCommit+1 to logBuf.length-1){
	     var bb  = ByteBuffer.allocate(264)			//get a new ByteBuffer
	     var data = logBuf(i)				//grab the current record to flush
	     bb.putInt(data._1)
	     bb.putInt(data._2)	       
	     if(data._3!=null) bb.put(data._3	)		//can't put(null) values
	     else 		 bb.put(("-"*128).getBytes())
	     if(data._4!=null) bb.put(data._4)			//again, null
	     else 		 bb.put(("-"*128).getBytes())	 
	     var ba = bb.array()
	     raf.seek(raf.length())				//make sure to be appending
     	     raf.write(ba)
	     println("written")
	}// while
    }
    
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
     var buf = Array.ofDim[Byte](264)
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
		data = logBuf(i)
		if( data._1 == tid ){
		    if( data._2 != BEGIN){
		    	val page       = data._2/32
		    	var cache_page = map.get(page)
		    	if(cache_page != None){
		    		  write(data._1, data._2, data._3);
		    	}// if
		    	else{
/*				val memPage = PDB.fetchPage(page)
				cache_page = victimizeCache(memPage);
*/ //TODO: Implement
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
    /** Replace a cache page with this page passed as a parameter.
     *  @param memPage  the page to replace the victim page 
     *
     */

    def victimizeCache(memPage: Page): Int =
    {
	0
    }
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
	// TODO implement init_store
	val log_file   = "log"
	val store_file = "store"
	try{
		val store = new RandomAccessFile(store_file,"rw")
		val log = new RandomAccessFile(log_file,"rw")
	}
	catch{
		case iae  : IllegalArgumentException => println("IllegalArgumentException: " + iae)
		case fnfe : FileNotFoundException    => println("FileNotFOundException in PDB: " + fnfe)
		case se   : SecurityException        => println("SecurityException: " + se)
	}
	def recover 
	{
	}
	def fetchPage(page: Int): VDB.Page =
	{
		val pg = VDB.Page()
		pg
	}
}

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDBTest` object is used to test the `VDB` object.
 *  > run-main trans.VDBTest
 */
object VDBTest extends App
{

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

