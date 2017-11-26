package net.example.anomalies.io

import java.util.Date

import net.example.anomalies.model.{ScoredPoint, DataPoint}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.{XContent, XContentBuilder, XContentFactory}

/**
  * Elastic search mapping for [[ScoredPoint]].
  *
  * @param index The elastic search index name.
  * @param typeName The type name in the index.
  */
class ScoredPointOutput(index : String, typeName : String) extends ElasticsearchSinkFunction[ScoredPoint]{
  def createIndexRequest(data: ScoredPoint) : IndexRequest = {
    val builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
        .field("timestamp", Date.from(data.dataPoint.timestamp))
        .field("sensor", data.dataPoint.sensor)
        .field("value", data.dataPoint.value)
        .field("anomalyScore", data.anomalyScore)
    Requests.indexRequest(index)
      .`type`(typeName)
      .source(builder)
  }

  override def process(point: ScoredPoint,
                       runtimeContext: RuntimeContext,
                       indexer: RequestIndexer): Unit =
    indexer.add(createIndexRequest(point))
}
