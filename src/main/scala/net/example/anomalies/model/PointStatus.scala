package net.example.anomalies.model

/**
  * Status for a corrected data point.
  */
object PointStatus extends Enumeration {

  type PointStatus = Value

  val Original, Interpolated, InterpolationFailure = Value

}
