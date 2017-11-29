package net.example.anomalies.model

import java.time.Instant
import java.util.UUID

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
  def isAnomalousOrMissing : Boolean

  /**
    * @return Unique record ID.
    */
  def id : UUID

}

/**
  * A good data point.
  * @param timestamp The timestamp of the data point.
  * @param value The value of the data point.
  * @param sensor The sensor from which the data point was taken.
  */
case class Good(override val id : UUID, override val timestamp : Instant, value : Double, override val sensor : String) extends FlaggedData {

  override lazy val epochTimeStamp: Long = timestamp.toEpochMilli

  override def isAnomalousOrMissing = false

}

/**
  * An anomalous data point.
  * @param timestamp The timestamp of the data point.
  * @param sensor The sensor from which the data point was taken.
  * @param anomalyLevel The anomaly score from the detector.
  */
case class Anomalous(override val id : UUID, override val timestamp : Instant,
                     override val sensor : String, anomalyLevel : Option[Double])
  extends FlaggedData {

  override lazy val epochTimeStamp : Long = timestamp.toEpochMilli

  override def isAnomalousOrMissing = true

}
