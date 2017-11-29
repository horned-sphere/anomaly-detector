package net.example.anomalies.io

import java.net.URI
import java.time.Instant
import java.util.UUID

import net.example.anomalies.model.DataPoint
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * Loads the data from a custom CSV format.
  * @param numSensors The number of sensors in the file.
  * @param path The path to the file.
  */
class CsvDataInputSource(numSensors : Int, path : URI, withSpaces : Boolean) extends InputSource with Serializable {

  import CsvDataInputSource._

  override def loadSource(env: StreamExecutionEnvironment): DataStream[DataPoint] = {
    //unpackRows(loadRows(env))


    val result = env.readFile(new TextInputFormat(new Path(path.toString)), path.toString, FileProcessingMode.PROCESS_CONTINUOUSLY, 5000)
      .map(_.split(",").map(_.trim)).flatMap { line =>
      if (line(0) != "Date") {
        val ts = Instant.parse(line(0))
        Some((ts, line.tail))
      } else None
    }.assignAscendingTimestamps(_._1.toEpochMilli)
      .flatMap { rec =>
        rec match {
          case (ts, line) => for (i <- 1 until line.length) yield {
            try {
              val sensor = s"Sensor-$i"
              val id = DataPoint.createId(sensor, ts)

              val value = if (line(i).isEmpty)
                None
              else Some(line(i).toDouble)

              DataPoint(id, ts, value, sensor)
            } catch {
              case ex : Exception => println(ex); throw(ex)
            }
          }
        }

      }
    result
  }

  def loadRows(env : StreamExecutionEnvironment): DataStream[Row] = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val builder = CsvTableSource.builder()
      //.ignoreParseErrors()
      .ignoreFirstLine()
      .path(path.toString)
      .field("Date", Types.STRING)

    val tableSrcBuilder = if(withSpaces) {
      builder.fieldDelimiter(", ")
        .lineDelimiter(" \n")
    } else builder

    for (i <- 1 to numSensors) tableSrcBuilder.field(s"Sensor-$i", Types.DOUBLE)

    tableEnv.registerTableSource("CsvInput", tableSrcBuilder.build())

    val table = tableEnv.scan("CsvInput")

    tableEnv.toAppendStream[Row](table)

  }

  def unpackRows(rows : DataStream[Row]) : DataStream[DataPoint] = {
    rows.map(r => (r, extractTimestamp(r)))
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
      DataPoint(DataPoint.createId(sensor, ts), ts, Some(value), sensor)
    }
  }

}
