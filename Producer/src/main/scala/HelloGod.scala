import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object HelloGod {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream: DataStream[String] = env.socketTextStream("hadoop102",9999)

    sourceStream.flatMap(x => x.split("\\W+"))
      .map(x => (x ,1))
      .keyBy(0)
      .sum(1)
      .print("result")

    env.execute("hello world")


  }

}
