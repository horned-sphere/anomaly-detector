package net.example.anomalies.model

import java.time.Instant

/**
  * Data that has been flagged as either good or anomalous.
  */
sealed trait FlaggedData {

  /**
    * @return The sensor to which the data belongs.
    */
  def sensor : String

  /**
    * @return Timestamp of the point.
    */
  def timestamp : Instant

  /**
    * @return Timestamp of the point.
    */
  def epochTimeStamp : Long

  /**
    * @return Is this record anomalous.
    */
  def isAnomalous : Boolean

}

/**
  * A good data point.
  * @param data The data.
  */
case class Good(data : DataPoint) extends FlaggedData {

  override def sensor: String = data.sensor

  override lazy val epochTimeStamp: Long = data.timestamp.toEpochMilli

  override def timestamp: Instant = data.timestamp

  override def isAnomalous = false
}

/**
  * An anomalous data point.
  * @param data The data.
  * @param anomalyLevel The anomaly score from the detector.
  */
case class Anomalous(data : DataPoint, anomalyLevel : Double) extends FlaggedData {
  override def sensor: String = data.sensor

  override lazy val epochTimeStamp : Long = data.timestamp.toEpochMilli

  override def timestamp: Instant = data.timestamp

  override def isAnomalous = false
}