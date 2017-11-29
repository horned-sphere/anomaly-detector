package net.example.anomalies.outliers

import net.example.anomalies.model.{DataPoint, FlaggedData, ScoredPoint}
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Strategy for flagging outliers in a stream of data points.
  */
trait OutlierStrategy {

  /**
    * Compute anomaly scores for a stream of data.
    * @param in The input data.
    * @return The scored data.
    */
  def scoreData(in : DataStream[DataPoint]) : DataStream[ScoredPoint]

  /**
    * Flag data as good and bad based on the anomaly score.
    */
  def flagPoint : ScoredPoint => FlaggedData

}
