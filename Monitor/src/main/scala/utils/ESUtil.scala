package utils

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost


object  ESUtil {

  //es 的一些安全配置 常量
  val ES_SECURITY_ENABLE = "es.security.enable"
  val ES_SECURITY_USERNAME = "es.security.username"
  val ES_SECURITY_PASSWORD = "es.security.password"


    def addSink[T](httpList: java.util.ArrayList[HttpHost],fuction : ElasticsearchSinkFunction[T] ,Parallelism : Int,
                   data: SingleOutputStreamOperator[T],): Unit = {

      val esSink = new ElasticsearchSink.Builder[T](httpList, fuction)
      esSink.setBulkFlushMaxActions()
    }


  val httpHosts = new util.ArrayList[HttpHost]()
  httpHosts.add(new HttpHost("hadoop01", 9200))

  val ds: DataStream[WaterSensor] = dataDS.map(
    s => {
      val datas = s.split(",")
      WaterSensor(datas(0), datas(1).toLong, datas(2).toDouble)
    }
  )

  val esSinkBuilder = new ElasticsearchSink.Builder[WaterSensor]( httpHosts, new ElasticsearchSinkFunction[WaterSensor] {
    override def process(t: WaterSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
      println("saving data: " + t)
      val json = new util.HashMap[String, String]()
      json.put("data", t.toString)
      val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
      requestIndexer.add(indexRequest)
      println("saved successfully")
    }


  }
