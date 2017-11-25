package net.example.anomalies.interpolation

import net.example.anomalies.model.{CorrectedDataPoint, FlaggedData}
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Strategy to interpolate to cover data that has been flagged as anomalous.
  */
trait InterpolationStrategy {

  /**
    * Attempt to interpolate missing data.
    * @param flagged The flagged data points.
    * @return The interpolated data.
    */
  def interpolate(flagged : DataStream[FlaggedData]) : DataStream[CorrectedDataPoint]

}
