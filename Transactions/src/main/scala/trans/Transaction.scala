/*
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
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` companion object
 */
object Transaction
{
    private var count = -1

    def nextCount () = { count += 1; count }

    //VDB.initCache ()

} // Transaction object

import Transaction._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
 *  @param sch  the schedule/order of operations for this transaction.
 */
class Transaction (sch: Schedule, concurrency: Int =1) extends Thread
{
    private val DEBUG       = true						// debug flag
    private val tid         = nextCount ()        		    		// tarnsaction identifier
    private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set
    private var numOps      = 0	   	 		    			// [ oid, (num_reads,num_writes)]
    private var contracting = false						// keeps track of which 2PL phase we're in (contracting or expanding)
    private var readLocks   = Map [Int, ReentrantReadWriteLock.ReadLock ]()	// (oid -> readLock)  read locks we haven't unlocked yet
    private var writeLocks  = Map [Int, ReentrantReadWriteLock.WriteLock]() 	// (oid -> writeLock) write locks we haven't unlocked yet 
    private val READ        = 0
    private val WRITE       = 1
    private val TSO         = 0
    private val _2PL        = 1

    private val ConcurrencyFlag = concurrency
    private var ROLLBACK = false
    private var thisSch = sch

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
     */
    override def run ()
    {
    	fillReadWriteSet()
        begin ()
	breakable{
		for (i <- sch.indices) {
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
	      numOps += 1
	}// for
    } // fillReadWriteSet


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
        if ( concurrency == TSO ) writeTSO(oid, value)
        else write2PL (oid, value)
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
     *  @param oid  the object/record being read
     */
    def read2PL (oid: Int): VDB.Record =
    {        
	var lock = new ReentrantReadWriteLock()

	LockTable.synchronized{
		lock  = LockTable.lock( oid )				// get the rrwl associated with this object from the lock table
	}
 	
	var readLock 	= lock.readLock()				// get the writeLock associated with the rrwl
	var writeLock 	= lock.writeLock()				// get the readLock associated with the rrwl
	var ret   	= Array.ofDim[Byte](128)

	if( writeLock.isHeldByCurrentThread() ){			// if you already hold the write lock, start reading
	    ret = (VDB.read (tid,oid))._1
	} // if
	else if( (rwSet contains oid) && (rwSet(oid)(1)>0) ){		// if you will need to write this item in the future, use the writeLock for read
	     writeLock.lock()	     					// try to lock the write lock
	     ret = (VDB.read (tid,oid))._1
     	     writeLocks += (oid -> writeLock)				// add the writeLock to your set of currently held writeLocks
	} // else if
	else{
		readLock.lock() 					// try to lock the read lock
		ret = (VDB.read(tid,oid))._1
		readLocks += (oid -> readLock)	 			// add the readLock to your set of currently held readLocks	     
	} // else
	
	rwSet(oid)(READ) -= 1						// take a read of this object off of the rw_set
 
	if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed 
	    (rwSet(oid)(WRITE) == 0)   ) rwSet -= oid
	   
	ret
    } // read
    

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
     *  @param oid    the object/record being written
     *  @param value  the new value for the the record
     */
    def write2PL (oid: Int, value: VDB.Record)
    {

	var lock = LockTable.lock(oid)
	var writeLock = lock.writeLock()
	if(writeLock.isHeldByCurrentThread) {
	    //println(s"already held a wrireLock for oid: $oid")
	    VDB.write(tid, oid, value)
	}
	else{
		writeLock.lock()
		/*
		println(s"adding to writeLocks, before: ")
		for(i <- writeLocks.keys) println(writeLocks(i))
		println(s"added to writeLocks, after: ")
		for(i <- writeLocks.keys) println(writeLocks(i))
		*/
		writeLocks += (oid -> writeLock)
		VDB.write(tid, oid, value)
	}
	rwSet(oid)(WRITE) -= 1
        if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                (rwSet(oid)(WRITE) == 0) ) rwSet -= oid
    } // write2PL

    def getsch (): Schedule ={

        this.sch
    }
    def getRollback(): Boolean={
        this.ROLLBACK
    }

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'.
      * Add Strict Timestamp Ordering
      *  @param oid  the object/record being read
      */

    def readTSO (oid: Int): VDB.Record =
    {
	val readTS = VDB.tsTable(oid)(1)
	if (tid < readTS){					//check if write_TS(X)<=TS(T), roll back T **************DID YOU DO THIS CORRECTLY? 
           VDB.rollback(tid)					// as it stands you have roll back if read_TS(X) < TS(T), is that correct? 
           null
    	} // if
    	else{							// else execute read and set read_TS(X)=TS(T)
        	VDB.tsTable(oid)(0) = tid
        	VDB.read (tid, oid)._1
    	}
        if (tid > VDB.tsTable(oid)(1))				//check if write_TS(X)<=TS(T), read using lock  ********** WHAT IS THIS FOR?**********
        {
            VDB.tsTable(oid)(1) = tid
            println("reading")

            var lock      = LockTable.lock(oid)					// get the rrwl associated with this object from the lock table
            var writeLock = lock.writeLock()            			// get the writeLock associated with the rrwl
	    var readLock  = lock.readLock()
            if (writeLock.isHeldByCurrentThread()) {            		// if you already hold the write lock, start reading
              return  (VDB.read(tid, oid))._1
            } // if
            /*   else if( (rwSet contains oid) && (rwSet(oid)(1)>0) ){		// if you will need to write this item in the future, use the writeLock for read
                writeLock.lock()	     					// try to lock the write lock
                (VDB.read (tid,oid))._1						// is this a return statement? 
                writeLocks += (oid -> prime_lock)				// if so how do you get here? 
            } // else if*/

            else{
                readLock.lock()						// try to lock the read lock
                return (VDB.read(tid,oid))._1
               // readLocks += (oid -> prime_lock2)			// are we locking without unlocking? 
            } // else

          //  rwSet(oid)(READ) -= 1						// take a read of this object off of the rw_set *** WHY NOT decrement the RW set? 

            if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                    (rwSet(oid)(WRITE) == 0) ) rwSet -= oid

            (VDB.read(tid,oid))._1


        }
        else
        {                                           // rollback if the current TS is bigger than write_TS, which means some yonger Tj has written oid
            rollback()
            null
        }
    } // readTSO

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO (oid: Int, value: VDB.Record)
    {
        if (tid > VDB.tsTable(oid)(1))         //check if current TSO >=write_TS, require the write lock
        {
	
            VDB.tsTable(oid)(1) = tid
            var lock = LockTable.lock(oid)
            var primeLock = lock.writeLock()
            if(primeLock.isHeldByCurrentThread) VDB.write(tid, oid, value)
            else{
                primeLock.lock()
                writeLocks += (oid -> primeLock)
                VDB.write(tid, oid, value)
            }
            rwSet(oid)(WRITE) -= 1
            if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                    (rwSet(oid)(WRITE) == 0) ) rwSet -= oid
        }
        else                                                       // else roll back since some younger transaton has written the oid
        {
            rollback()
        }
    } // writeTSO
    
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
        //if (DEBUG) println (VDB.logBuf)
    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
     */
    def rollback ()
    {   ROLLBACK= true
        VDB.rollback (tid)
        releaseReadLocks()
        releaseWriteLocks()
    } // rollback

    
} // Transaction class

 

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `TransactionTest` object is used to test the `Transaction` class.
 *  > run-main trans.TransactionTest
 */
object TransactionTest extends App
{   import scala.util.Random
    private val _2PL = 0
    private val TSO = 1
    private val transactionNum =20
    private val opPerTran = 23
    val t1 = new Transaction (new Schedule (List ( ('r', 0, 0), ('r', 0, 1), (w, 0, 0), (w, 0, 1) )),TSO)
    val t2 = new Transaction (new Schedule (List ( ('r', 1, 0), ('r', 1, 1), (w, 1, 0), (w, 1, 1) )),TSO)


    //generate transactions
    val a = Array.ofDim[Transaction](50)
    for (i <-0 to transactionNum)
        {
            var l: List[(Char,Int,Int)]= List()
            for (j <- 0 to opPerTran)
            {   var k='x'
                if ( Random.nextDouble()>0.5)
                {k='r'}
                 else
                {k = 'w'}

                 val b=Random.nextInt(10)
                l =  (k,0,b) :: l
            }
            a(i) =new Transaction (new Schedule (l),TSO)

        }

    VDB.initCache()
    for (i<-0 to 20)
    a(i).start()

    /// generate new transaction if rollback
    var rollback = new ArrayBuffer[Int]
    for (i<-0 to 20)
       if  (a(i).getRollback()) {
           rollback += i

       }
    val r = Array.ofDim[Transaction](rollback.size)
    for (i<-0 to rollback.size){
       r(i) = new Transaction( a(rollback(i)).getsch(),TSO)

    }


   /* for (t1<-transactions){
        if (t1.ROLLBACK==true) {
        val x = new Transaction (t1.getsch())
        x.start()}
    }*/


} // TransactionTest object

