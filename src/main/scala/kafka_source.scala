import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._


object kafka_source {


  def main(args: Array[String]): Unit = {



    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val kafkaDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties)
    )

    kafkaDS.print()

    env.execute("sensor")

  }

}
