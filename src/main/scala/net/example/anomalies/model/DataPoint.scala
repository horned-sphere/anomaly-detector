package net.example.anomalies.model

import java.time.Instant

/**
  * A raw data point.
  * @param timestamp The timestamp of the data point.
  * @param value The value of the data point.
  * @param sensor The sensor from which the data point was taken.
  */
case class DataPoint(timestamp : Instant, value : Double, sensor : String)
