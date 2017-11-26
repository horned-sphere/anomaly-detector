package net.example.anomalies.model

/**
  * Description of an anomaly.
  * @param dataPoint The underlying point.
  * @param anomalyScore The anomaly score from the anomaly detector.
  */
case class AnomalousPoint(dataPoint : DataPoint, anomalyScore : Double)
