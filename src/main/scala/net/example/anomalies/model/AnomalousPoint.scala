package net.example.anomalies.model

import java.time.Instant

/**
  * Description of an anomaly.
  * @param sensor The sensor on which the anomaly occurred.
  * @param timestamp The timestamp of the anomaly.
  * @param anomalyScore The anomaly score from the anomaly detector.
  */
case class AnomalousPoint(sensor : String, timestamp : Instant, anomalyScore : Double)
