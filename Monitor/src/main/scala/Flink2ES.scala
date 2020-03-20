import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import utils.UserInfo
import org.apache.flink.api.scala._

/**
  * flink 将处理完的数据灌入ES 中
  */
object Flink2ES {

  def main(args: Array[String]): Unit = {

    val env =  StreamExecutionEnvironment.getExecutionEnvironment



    val httpHosts = new java.util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200) )
    val sink = new ElasticsearchSink.Builder[UserInfo](httpHosts, new ElasticsearchSinkFunction[UserInfo] {
      /**
        *
        * @param t              存入的数据类型
        * @param runtimeContext 上下文对象
        * @param requestIndexer 请求的索引
        */
      override def process(t: UserInfo, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("启动程序, 开始存数据~~")

        val json = new java.util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")

      }
    })

    //添加数据源
    env.addSource(new SourceFunction[UserInfo] {
      override def run(ctx: SourceFunction.SourceContext[UserInfo]): Unit =


      override def cancel(): Unit = ???
    })
    sink







  }


}
