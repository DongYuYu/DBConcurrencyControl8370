
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller
 *  @version 1.2
 *  @date    Tue Jan 24 14:31:26 EST 2017
 *  @see     LICENSE (MIT style license file).
 */

package trans

import scala.collection.mutable.{ArrayBuffer, Map}



import java.io.{IOException, RandomAccessFile, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock





import Array._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `VDB` object represents the Volatile Database.
 */
object VDB
{
    type Record = Array [Byte]                           // record type
    type LogRec = Tuple4 [Int, Int, Record, Record]      // log record type (tid, oid, v_old, v_new)


    private val DEBUG         = true                     // debug flag
    private val pages         = 5                        // number of pages in cache
    private val recs_per_page = 32                       // number of record per page
    private val record_size   = 128                      // size of record in bytes

    private val BEGIN    = -1
    private val COMMIT   = -2
    private val ROLLBACK = -3

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
            var tsTable = ofDim[Int](pages*recs_per_page, 2)  //create a Timestamp Table record the (rts, ws) of oid
           // var tsTable = Array.ofDim[TS] (pages*recs_per_page)  //Timestamp table
    private val map    = Map [Int, Int] ()               // map for finding pages in cache

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Initialize the cache.
     */
    def  initCache ()
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
        val cpi = map(oid / recs_per_page)         // the cache page index
        val pg = cache(cpi)                        // page in cache
        (pg.p(oid % recs_per_page), cpi)           // record, location in cache
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the 'newVal' record to the database.
     *  @param tid  the transaction performing the write operation
     *  @param oid  the object/record being written
     */
    def write (tid: Int, oid: Int, newVal: Record)
    {
        if (DEBUG) println (s"write ($tid, $oid, $newVal)")
        val (oldVal, cpage) = read (tid, oid)
        logBuf += ((tid, oid, oldVal, newVal))
        val pg = cache(map(oid / recs_per_page))
        pg.p(oid % recs_per_page) = newVal
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
        if (DEBUG) println (s"commit ($tid)")
        //seek the last commit or rollback and write each item to current commit?
        logBuf += ((tid, COMMIT, null, null))
        // I M P L E M E N T
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback the transaction with id 'tid'.
     *  @param tid  the transaction id
     */
    def rollback (tid: Int)
    {
        if (DEBUG) println (s"rollback ($tid)")
        logBuf += ((tid, ROLLBACK, null, null))
        // I M P L E M E N T
    } // rollback

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Generate the ith record.
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

   // println ("\nTest reads and writes:")
    println ("read (2, 40)")
    println (new String (VDB.read (2, 40)._1))
    val newVal = VDB.str2record ("new value for record 40 ")
    println (s"write (2, 40, ${new String (newVal)})")
    VDB.write (2, 40, newVal)
    println ("read (2, 40)")
    println (VDB.read (2, 40)._2)
    
    println ("\nPrint cache")
    for (pg <- VDB.cache; rec <- pg.p) println (new String (rec))   // as text

} // VDBTest

