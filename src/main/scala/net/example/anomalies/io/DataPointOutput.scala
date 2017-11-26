package net.example.anomalies.io

import java.util.Date

import net.example.anomalies.model.DataPoint
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentFactory

/**
  * Elastic search mapping for [[DataPoint]].
  * @param index The elastic search index name.
  * @param typeName The type name in the index.
  */
class DataPointOutput(index : String, typeName : String) extends ElasticsearchSinkFunction[DataPoint]{

  def createIndexRequest(data : DataPoint) : IndexRequest = {
    val builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
      .field("timestamp", Date.from(data.timestamp))
      .field("sensor", data.sensor)
      .field("value", data.value)

    Requests.indexRequest(index)
      .`type`(typeName)
      .source(builder)
  }

  override def process(point : DataPoint,
                       runtimeContext: RuntimeContext,
                       indexer: RequestIndexer): Unit =
    indexer.add(createIndexRequest(point))
}
