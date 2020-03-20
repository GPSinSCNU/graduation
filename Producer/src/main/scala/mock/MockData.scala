package mock

import org.joda.time.DateTime

import scala.util.Random


object MockData {

  def main(args: Array[String]): Unit = {
    val random = new Random()
    println(random.nextInt(100))

    val time = DateTime.now().toString("yyyy-MM-dd HH:mm:ss")
    val userid = (random.nextInt(100) + 100).toString
    val itemid = (random.nextInt(1000) + 1000).toString
    val categoryid = (random.nextInt(10000) + 10000).toString
    val behaviour = "pv";

    val output = s"{userid:$userid,itemid:$itemid,categoryid:$categoryid,behaviour:$behaviour,time:$time}"
    println(output)

  }




}
