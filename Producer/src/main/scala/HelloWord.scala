import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object HelloWord {

  def main(args: Array[String]): Unit = {

    val hostname = args(0)
    val port = args(1)
    //获取流上下文对象
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream: scala.DataStream[String] = env.socketTextStream(hostname,port.toInt)

      sourceStream.flatMap( x => x.split("\\W+"))
      .map(x =>  (x,1))
      .keyBy(0)
      .sum(1)
      .print()

      //执行
      env.execute("my (fake)first flink job")


  }


}
