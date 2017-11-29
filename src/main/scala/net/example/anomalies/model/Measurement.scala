package net.example.anomalies.model

/**
  * Type class for measurements.
  * @tparam T The type.
  */
trait Measurement[T] {

  /**
    * Get the value of the measurement.
    * @param record The record.
    * @return The measurement.
    */
  def getValue(record : T) : Option[Double]

}

object Measurement {

  implicit object DoubleMeasurement extends Measurement[Double] {

    override def getValue(record: Double): Option[Double] = Some(record)
  }

}
