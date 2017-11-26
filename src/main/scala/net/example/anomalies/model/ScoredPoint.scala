package net.example.anomalies.model

/**
  * A data point with associated anomaly score.
  * @param dataPoint The underlying point.
  * @param anomalyScore The anomaly score from the anomaly detector.
  */
case class ScoredPoint(dataPoint : DataPoint, anomalyScore : Double)
