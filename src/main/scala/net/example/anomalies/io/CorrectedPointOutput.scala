package net.example.anomalies.io

import java.util.Date

import net.example.anomalies.model.CorrectedDataPoint
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

/**
  * Elastic search mapping for [[CorrectedDataPoint]].
  * @param index The elastic search index name.
  * @param typeName The type name in the index.
  */
class CorrectedPointOutput(index : String, typeName : String) extends ElasticsearchSinkFunction[CorrectedDataPoint]{
  def createIndexRequest(data : CorrectedDataPoint) : IndexRequest = {
    val builder = extract(XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
      .field("timestamp", Date.from(data.timestamp))
      .field("sensor", data.sensor)
      .field("status", data.status), data.value)

    Requests.indexRequest(index)
      .`type`(typeName)
      .source(builder)
  }

  def extract(builder : XContentBuilder, value : Option[Double]) : XContentBuilder = value match {
    case Some(v) => builder.field("value", v)
    case _ => builder.nullField("value")
  }

  override def process(point : CorrectedDataPoint,
                       runtimeContext: RuntimeContext,
                       indexer: RequestIndexer): Unit =
    indexer.add(createIndexRequest(point))
}
