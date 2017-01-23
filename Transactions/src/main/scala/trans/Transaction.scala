package trans
import Operation._
import Schedule._

class Transaction(tid: Int, sch: Schedule) extends Thread
{
	override def run()
	{
		for(i<-sch) println(i)
	}
}

object TransactionTest extends App
{
	val s1 = new Schedule (List ( (r, 0, 0), (r, 0, 1), (w, 0, 0), (w, 0, 1) ))
	val s2 = new Schedule (List ( (r, 1, 0), (r, 1, 1), (w, 1, 0), (w, 1, 1) ))

	val t1 = new Transaction(1, s1);
	val t2 = new Transaction(2, s2);
	
	t1.start()
	t2.start()

	val s3 = genSchedule(5,5,5)
	val s4 = genSchedule(5,4,3)

	println("s3: " + s3)
	println("s4: " + s4)
}