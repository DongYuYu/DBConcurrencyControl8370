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

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `Transaction` class
  *  @param sch  the schedule/order of operations for this transaction.
  */
class Transaction (sch: Schedule, concurrency: Int =0) extends Thread
{
    private val DEBUG       = true						// debug flag
private val tid         = nextCount ()        		    		// tarnsaction identifier
private var rwSet       = Map[Int, Array[Int]]()		    		// the read write set
private var numOps      = 0	   	 		    			// [ oid, (num_reads,num_writes)]
private var contracting = false						// keeps track of which 2PL phase we're in (contracting or expanding)
private var readLocks   = Map [Int, ReentrantReadWriteLock.ReadLock ]()	// set of read locks we haven't unlocked yet and the oid they apply to
private var writeLocks  = Map [Int, ReentrantReadWriteLock.WriteLock]() 	// set of write locks we haven't unlocked yet and the oid they apply to

private val READ        = 0
    private val WRITE       = 1
    private val ConcurrencyFlag = concurrency
    private var ROLLBACK = false; private var thisSch= sch

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Run this transaction by executing its operations.
      */
    override def run ()
    {
        fillReadWriteSet()
        begin ()


        breakable{


		for (i <- sch.indices) {
            if (!ROLLBACK) {
                val (op, tid, oid) = sch(i)
                //if(DEBUG) println (sch(i))
                if (op == r) read(oid)
                else write(oid, VDB.str2record(sch(i).toString))
            } // if
            else break
        }
            // for
	}
        if(!ROLLBACK) commit ()

    } // run

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Fills the read/write set for this transaction.
      */
    def fillReadWriteSet()
    {
        for(i <- sch.indices){
            val op = sch(i)
            if (rwSet contains op._3){
                if( op._1 == r ) rwSet(op._3)(READ) += 1		//increment the read value for this object in the readWriteSet
                else             rwSet(op._3)(WRITE) += 1		//increment the write value
            } // if
            else{
                var tup = Array.fill(2)(0)				//add a new member to the read write set
                if( op._1 == r ) tup(READ) = 1				//make it a read member
                else             tup(WRITE) = 1				//make it a write member

                rwSet += (op._3 -> tup)
            } // else
            numOps += 1
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
        var ret   = Array.ofDim[Byte](128)
        LockTable.synchronized{
            lock  = LockTable.lock( oid )				// get the rrwl associated with this object from the lock table
        }

        var prime_lock = lock.writeLock()				// get the writeLock associated with the rrwl
        if( prime_lock.isHeldByCurrentThread() ){			// if you already hold the write lock, start reading
            ret = (VDB.read (tid,oid))._1
        } // if
        else if( (rwSet contains oid) && (rwSet(oid)(1)>0) ){		// if you will need to write this item in the future, use the writeLock for read
            prime_lock.lock()	     					// try to lock the write lock
            ret = (VDB.read (tid,oid))._1
            writeLocks += (oid -> prime_lock)
        } // else if
        else{
            var prime_lock2 = lock.readLock()			// switch to the read lock b/c you don't need to write in the future
            prime_lock2.lock() 					// try to lock the read lock
            ret = (VDB.read(tid,oid))._1
            readLocks += (oid -> prime_lock2)
        } // else

        rwSet(oid)(READ) -= 1						// take a read of this object off of the rw_set

        if( (rwSet(oid)(READ) == 0) &&					// remove the oid from the rwSet if no more reads or writes needed
                (rwSet(oid)(WRITE) == 0) ) rwSet -= oid

        ret
    } // read


    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Unlock any read locks this transaction may own.
      */
    def releaseReadLocks()
    {
        for( lock <- readLocks ){

            lock._2.unlock()						//unlock the lock

            LockTable.checkLock(lock._1)				//remove the lock from the lock table if necessary
        }
    } // releaseReadLocks


    this.synchronized{

    }
    //:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Unlock any write locks this transaction may own.
      */
    def releaseWriteLocks()
    {
        for( lock <- writeLocks ){
            if (lock._2.isHeldByCurrentThread)

            {
                lock._2.unlock()						//unlock the lock

                LockTable.checkLock(lock._1)

            }			//remove the lock from the lock table if necessary
        }
    } // releaseWriteLocks

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write2PL (oid: Int, value: VDB.Record)
    {

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

    } // write

    def deadlockChecker() ={

    }
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
    def readTS (oid: Int): VDB.Record =
    {
        var read_TS = VDB.tsTable(oid)(1)

        if (tid >= read_TS) {
            if (tid > VDB.tsTable(oid)(1)) {
                var lock = new ReentrantReadWriteLock()

                println("reading")
                VDB.tsTable(oid)(1) = tid
                LockTable.synchronized{
                    lock  = LockTable.lock( oid )				// get the rrwl associated with this object from the lock table
                }
                var prime_lock = lock.readLock()
                prime_lock.lock()

                readLocks += (oid -> prime_lock)

                return (VDB.read(tid, oid))._1
            }
            else {
                return (VDB.read(tid, oid))._1
            }
        }
        else{
                rollback()
                null
            } // else
    } // read

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'.
      * Add Strict Time Stamp Ordering
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def writeTSO (oid: Int, value: VDB.Record)
    {
        if (tid >= VDB.tsTable(oid)(1) && tid >= VDB.tsTable(oid)(0))         //check if current TSO >=write_TS, require the write lock
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

        }
        else                                                       // else roll back since some younger transaton has written the oid
        {
            rollback()
        }
    } // write
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Read the record with the given 'oid'. Redirect to different concurrency by ConcurrencyFlag setting
      *  @param oid  the object/record being read
      */
    def read (oid: Int) :VDB.Record ={
        if (ConcurrencyFlag==1) readTS(oid)
        else read2PL(oid)
    }
    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Write the record with the given 'oid'. Redirect to different concurrency control by ConcurrencyFlag setting
      *  @param oid    the object/record being written
      *  @param value  the new value for the the record
      */
    def write (oid:Int, value:VDB.Record  ) ={
        if (ConcurrencyFlag ==1 ) writeTSO(oid, value)
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
        if (DEBUG) println (VDB.logBuf)

        releaseReadLocks()
        releaseWriteLocks()

    } // commit

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Rollback this transaction.
      */
    def rollback ()
    {   ROLLBACK= true
        VDB.rollback (tid)
        releaseReadLocks()
        releaseWriteLocks()
        var t = new Transaction(sch)
        t.start()
    } // rollback

    
} // Transaction class

object waitGraph{


}
 class waitGraph  extends Thread{
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


     }







}

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
    val transactions = Array.ofDim[Transaction](50)
    for (i <-0 to transactionNum)
    {
        var sched = List[(Char,Int,Int)]()
        for (j <- 0 to opPerTran)
        {   var k='x'
            if ( Random.nextDouble()>0.5)
            {k='r'}
            else
            {k = 'w'}

            val b=Random.nextInt(10)
            sched =  (k,0,b) :: sched
        }
        transactions(i) =new Transaction (new Schedule (sched),TSO)

    }

    VDB.initCache()
    for (i<-0 to 20)
        transactions(i).start()

    /// generate new transaction if rollback
    var rollback = new ArrayBuffer[Int]
    for (i<-0 to 20)
        if  (transactions(i).getRollback()) {
            rollback += i

        }
    val r = Array.ofDim[Transaction](rollback.size)
    for (i<-0 to rollback.size){
        if (rollback.size==0) println("no rollback")
        else r(i) = new Transaction( transactions(rollback(i)).getsch(),TSO)

    }


    /* for (t1<-transactions){
         if (t1.ROLLBACK==true) {
         val x = new Transaction (t1.getsch())
         x.start()}
     }*/


} // TransactionTest object

object textAdjustlock extends App{

    var t1 = new waitGraph()
    var t2 = new waitGraph()

    t1.start()
    t2.start()
    var test = new adjustLock()
    test.writeLock().lock()
    println(test.getOwner.getName)

}