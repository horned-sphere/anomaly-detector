package net.example.anomalies.outliers

import net.example.anomalies.model.{DataPoint, FlaggedData}
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Strategy for flagging outliers in a stream of data points.
  */
trait OutlierStrategy {

  /**
    * Transform a stream of data points into a stream of flagged points.
    * @param in The input data.
    * @return The flagged data.
    */
  def flagOutliers(in : DataStream[DataPoint]) : DataStream[FlaggedData]

}
