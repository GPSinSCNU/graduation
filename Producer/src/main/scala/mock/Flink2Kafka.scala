import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleSerializers
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.joda.time.DateTime

import scala.util.Random

object Flink2Kafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //产生随机数
    val random = new Random()
    env.addSource(new SourceFunction[String] {
      //写入自动生成的流式数据
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (true) {
          val time = DateTime.now().toString("yyyy-MM-dd HH:mm:ss")
          val userid = (random.nextInt(100) + 100).toString
          val itemid = (random.nextInt(1000) + 1000).toString
          val categoryid = (random.nextInt(10000) + 10000).toString
          val behaviour = "pv";

          val output = s"{userid:$userid,itemid:$itemid,categoryid:$categoryid,behaviour:$behaviour,time:$time}"

          ctx.collect(output)
          Thread.sleep(200)
        }
      }


      override def cancel(): Unit = {
      }

    })
      .addSink(new FlinkKafkaProducer011[String](
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "graduation_topic",
        new SimpleStringSchema()
      )).name("flink-connector-kafka011")


    env.execute("flink-sink-to-kafka")


  }


}
