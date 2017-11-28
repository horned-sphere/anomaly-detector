package net.example.anomalies.model

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.time.Instant
import java.util.UUID

/**
  * A raw data point.
  * @param timestamp The timestamp of the data point.
  * @param value The value of the data point.
  * @param sensor The sensor from which the data point was taken.
  */
case class DataPoint(id : UUID, timestamp : Instant, value : Double, sensor : String)

object DataPoint {

  /**
    * Create a unique ID for a record.
    * @param sensor The sensor name.
    * @param timestamp The timestamp.
    * @return The ID.
    */
  def createId(sensor : String, timestamp : Instant): UUID = {

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    dos.writeUTF(sensor)
    dos.writeLong(timestamp.toEpochMilli)

    dos.close()
    bos.close()

    UUID.nameUUIDFromBytes(bos.toByteArray)
  }

  implicit object DataPointExtractor extends Timestamped[DataPoint] with Measurement[DataPoint] {
    override def getTimestamp(record: DataPoint): Long = record.timestamp.toEpochMilli

    override def getValue(record: DataPoint): Double = record.value
  }
}
