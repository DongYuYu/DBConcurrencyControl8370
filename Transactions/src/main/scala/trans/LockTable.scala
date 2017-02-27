
/***********************************************************************************
 * @author  John Miller
 * @version 1.0
 * @date    Wed Feb 10 18:41:29 EST 2010
 */

package trans

import scala.collection.mutable.HashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantReadWriteLock
/***********************************************************************************
 * This class implements a lock table as a hash map.  The object identifier (oid)
 * is used as the key to find the lock in the hash table.  If the lock is not found,
 * the data object is not locked.  This class can be used in the implementation
 * of locking protocols, such as Two-Phase Locking (2PL).
 * Caveat: shared/read locks are currently not implemented.
 */
class LockTable2
{


    /** Associative map of locks associated with objects K => V :: (oid => lock)
     */
    private var locks = new HashMap [Int, ReentrantReadWriteLock] ()

    /** An associate map of locks to the owners of the locks: K => V :: (oid => set of owners )
     */
    private var owners = new HashMap [Int, Set[Int]] ()

    /** Retrieve the lock associated with an object
     */
    def getObjLock(oid: Int) : ReentrantReadWriteLock = {
    	synchronized{
		if ( locks contains oid ) locks(oid)
		else{
			val lock  = new ReentrantReadWriteLock(true)
			locks += ((oid,lock))
			lock
		} // else
	} // synchronized
    }

    def getOwners(oid: Int): Set[Int] =
    {
	synchronized{
		if( owners contains oid ) owners(oid)
		else Set[Int] ()
	}
    }

    def addOwner(oid: Int, tid: Int)
    {
	synchronized{
		if(owners contains oid) owners(oid) += tid
		else owners(oid) = Set(tid)
	}
    }

    /*******************************************************************************
     * Unlock/release the lock on data object oid.
     * @param tid  the transaction id
     * @param oid  the data object id
     */
    def unlock (tid: Int, oid: Int)
    {
	synchronized{
	    if( (owners contains oid) && (owners(oid) contains tid) ) {
	    	owners(oid) -= tid
		if( owners(oid).size == 0 ) owners = owners - oid
	    } // if
	    else if( !(owners contains oid) ) println(s"$tid tried to unlock object $oid which didn't have any owners")
	    else println(s"$tid tried to unlock object $oid that it didn't own.")
	} // synchronized
    } // ul

    /*******************************************************************************
     * Convert the lock table to a string.
     */
    override def toString: String =
    {
        synchronized {
	    var ret = "LockTable: \n"
	    for(oid <- locks.keys){
	    	    ret += (oid + ": ")
		    ret += owners.get(oid).mkString(" ")
		    ret += "\n"		    
	    }
	    ret
       } // synchronized
    } // toString

} // LockTable class


/***********************************************************************************
 * Test the LockTable class.  Note, typically an object of this class would would
 * be accessed by multiple threads.
 */
object LockTableTest extends App
{
    println(s"Creating new lock table")
    val ltab = new LockTable2 ()
    println(s"Creating new locks for first time.")
    val lock1 = ltab.getObjLock(1)
    val lock2 = ltab.getObjLock(2)
    val lock3 = ltab.getObjLock(3)
    println(s"Created.")
    println(ltab)
    println(s"Creating owners for the locks.")
    ltab.addOwner(1,1)
    ltab.addOwner(2,2)
    ltab.addOwner(3,3)
    println(ltab)
    println(s"Accessing already created locks.")
    for(i <- 1 to 3){
        var lock4 = ltab.getObjLock(i)
    } // for 
    println(s"Adding more owners to the locks")
    for(i <- 4 to 6) ltab.addOwner(i-3,i)
    println(ltab)
    println(s"Unlocking the locks round 1")
    for(i <- 1 to 3) ltab.unlock(i,i)
    println(ltab)
    println(s"Unlocking the locks round 2")
    for(i <- 4 to 6) ltab.unlock(i,i-3)
    println(ltab)
} // LockTable object

