package net.example.anomalies.io


import java.time.Instant

import org.scalatest._

import scala.util.matching.Regex

/**
  * Test for the [[CsvDataInputSource]].
  */
class CsvInputTest extends FunSpec with Matchers with Inside {

  import CsvInputTest._

  describe("The CSV input") {

    it("should ignore header lines") {

      CsvDataInputSource.cleanRow(Rows.head) shouldEqual None

    }

    it("should clean lines correctly") {
      inside(CsvDataInputSource.cleanRow(Rows(1))) {
        case Some((ts, cols)) =>
          ts shouldEqual Instant.parse("2017-01-01T00:00:00Z")
          cols shouldEqual Array("1.0", "2.0", "3.0", "4.0")
      }

      inside(CsvDataInputSource.cleanRow(Rows(2))) {
        case Some((ts, cols)) =>
          ts shouldEqual Instant.parse("2017-01-01T00:00:10Z")
          cols shouldEqual Array("2.0", "4.0", "", "8.0")
      }
    }

    it("should create the correct data points for good data") {

      val points = CsvDataInputSource.cleanRow(Rows(1)).toList.flatMap {
        case (ts, cols) => CsvDataInputSource.unpackSensorReadings(ts, cols)
      }

      val expectedTs = Instant.parse("2017-01-01T00:00:00Z")

      for ((point, i) <- points.zipWithIndex) {
        point.timestamp shouldEqual expectedTs
        inside(point.sensor) {
          case Sensor(n) => n.toInt shouldEqual i + 1
        }
        point.value shouldEqual Some((i + 1).toDouble)
      }
    }

    it("should create the correct data points for missing data") {

      val points = CsvDataInputSource.cleanRow(Rows(2)).toList.flatMap {
        case (ts, cols) => CsvDataInputSource.unpackSensorReadings(ts, cols)
      }

      val expectedTs = Instant.parse("2017-01-01T00:00:10Z")

      for ((point, i) <- points.zipWithIndex) {
        point.timestamp shouldEqual expectedTs
        inside(point.sensor) {
          case Sensor(n) => n.toInt shouldEqual i + 1
        }
        if (i == 2) {
          point.value shouldEqual None
        } else {
          point.value shouldEqual Some((2 * (i + 1)).toDouble)
        }
      }
    }

  }

}

object CsvInputTest {

  val Rows : List[String] = List(
    "Date, Sensor-1, Sensor-2, Sensor-3, Sensor-4 ",
    s"2017-01-01T00:00:00Z, 1.0, 2.0, 3.0, 4.0 ",
    s"2017-01-01T00:00:10Z, 2.0, 4.0, , 8.0 "
  )

  val Sensor: Regex = "Sensor-(\\d+)".r


}
