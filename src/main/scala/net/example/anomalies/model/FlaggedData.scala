package net.example.anomalies.model

/**
  * Data that has been flagged as either good or anomalous.
  */
sealed trait FlaggedData {

  /**
    * @return The sensor to which the data belongs.
    */
  def sensor : String

}

/**
  * A good data point.
  * @param data The data.
  */
case class Good(data : DataPoint) extends FlaggedData {
  override def sensor: String = data.sensor
}

/**
  * An anomalous data point.
  * @param data The data.
  * @param anomalyLevel The anomaly score from the detector.
  */
case class Anomalous(data : DataPoint, anomalyLevel : Double) extends FlaggedData {
  override def sensor: String = data.sensor
}