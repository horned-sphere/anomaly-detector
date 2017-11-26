package net.example.anomalies.model

import java.time.Instant
import java.util.UUID

import net.example.anomalies.model.PointStatus.PointStatus

/**
  * Type representing a data point which has potentially been corrected.
  * @param id The record ID.
  * @param timestamp The timestamp of the data point.
  * @param value The value of the data point.
  * @param sensor The name of the sensor.
  * @param status Whether the data point is original, interpolated or omitted (with reason)
  */
case class CorrectedDataPoint(id : UUID,
                              timestamp : Instant,
                              value : Option[Double],
                              sensor : String,
                              status : PointStatus)
