package net.example.anomalies.model

/**
  * Type class for timestamped records.
  * @tparam T The type.
  */
trait Timestamped[T] {

  /**
    * Get the timestamp.
    * @param record The record.
    * @return The timestamp.
    */
  def getTimestamp(record : T) : Long

}

object Timestamped {

  implicit object LongTimestamp extends Timestamped[Long] {
    override def getTimestamp(record: Long): Long = record
  }

}
