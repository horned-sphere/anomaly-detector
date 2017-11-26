package net.example.anomalies.io

import java.net.URI
import java.time.Instant

import net.example.anomalies.model.DataPoint
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * Loads the data from a custom CSV format.
  * @param numSensors The number of sensors in the file.
  * @param path The path to the file.
  */
class CsvDataInputSource(numSensors : Int, path : URI) extends InputSource {

  import CsvDataInputSource._

  override def loadSource(env: StreamExecutionEnvironment): DataStream[DataPoint] = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val tableSrcBuilder = CsvTableSource.builder()
      .ignoreParseErrors()
      .ignoreFirstLine()
      .path(path.toString)
      .field("Date", Types.STRING)

    for (i <- 1 to numSensors) tableSrcBuilder.field(s"Sensor-$i", Types.DOUBLE)

    tableEnv.registerTableSource("CsvInput", tableSrcBuilder.build())

    val table = tableEnv.scan("CsvInput")

    tableEnv.toAppendStream[Row](table)
        .map(r => (r, extractTimestamp(r)))
        .assignAscendingTimestamps(_._2.toEpochMilli)
        .flatMap(fromRow(numSensors) _)
  }
}

object CsvDataInputSource {

  import java.lang.{Double => JDouble}

  /**
    * Extract the timestamp from a row of the file.
    * @param row The row.
    * @return The timestamp.
    */
  def extractTimestamp(row : Row) : Instant = Instant.parse(row.getField(0).asInstanceOf[String])

  /**
    * Create a list of data points from a single row of the file, one for each sensor.
    * @param numSensors The number of sensors.
    * @param rowWithTs The row and timestamp.
    * @return The data points.
    */
  def fromRow(numSensors : Int)(rowWithTs : (Row, Instant)) : List[DataPoint] = {

    val (row, ts) = rowWithTs

    for (i <- (1 to numSensors).toList) yield {
      val value = row.getField(i).asInstanceOf[JDouble].doubleValue()
      val sensor = s"Sensor-$i"
      DataPoint(DataPoint.createId(sensor, ts), ts, value, sensor)
    }
  }

}
