package net.example.anomalies.io

import java.net.URI
import java.time.Instant

import net.example.anomalies.model.DataPoint
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Try

/**
  * Loads the data from a custom CSV format.
  * @param path The path to the file.
  */
class CsvDataInputSource(path : URI) extends InputSource with Serializable {

  import CsvDataInputSource._

  override def loadSource(env: StreamExecutionEnvironment): DataStream[DataPoint] = {
    env.readFile(new TextInputFormat(new Path(path.toString)), path.toString, FileProcessingMode.PROCESS_CONTINUOUSLY, 5000)
      .flatMap(row => cleanRow(row))
      .assignAscendingTimestamps(_._1.toEpochMilli)
      .flatMap(row => row match {
        case (ts, measurements) => unpackSensorReadings(ts, measurements)
      })
  }

}

object CsvDataInputSource {

  /**
    * Clean a CSV row and extract the timestamp.
    * @param row The row.
    * @return The timestamp and remaining fields.
    */
  def cleanRow(row : String) : Option[(Instant, Array[String])] = {
    val parts = row.split(",").map(_.trim)
    val tsOpt = Try(Instant.parse(parts(0))).toOption
    tsOpt.map(ts => (ts, parts.tail))
  }

  /**
    * Unpack the readings from the remaining columsn.
    * @param ts The timestamp.
    * @param measurements The columns.
    * @return The data points.
    */
  def unpackSensorReadings(ts : Instant, measurements : Array[String]) : Iterable[DataPoint] = {
    for (i <- measurements.indices) yield {
      val sensor = s"Sensor-${i + 1}"
      val id = DataPoint.createId(sensor, ts)

      val value = if (measurements(i).isEmpty)
        None
      else {
        Try(measurements(i).toDouble).toOption
      }

      DataPoint(id, ts, value, sensor)
    }
  }

}

