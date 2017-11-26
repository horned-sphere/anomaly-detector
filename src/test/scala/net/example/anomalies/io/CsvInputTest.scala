package net.example.anomalies.io

import java.net.URI

import io.flinkspector.core.runtime.OutputVerifier
import io.flinkspector.datastream.DataStreamTestBase
import net.example.anomalies.model.DataPoint
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.types.Row
import org.scalatest.{BeforeAndAfter, FunSpecLike, Inside, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Test for the [[CsvDataInputSource]].
  */
class CsvInputTest extends DataStreamTestBase with FunSpecLike with Matchers with BeforeAndAfter with Inside {

  import CsvInputTest._

  before {
    initialize()

  }

  describe("The CSV input") {

    it("should produce the correct number of records in time order") {

      import scala.collection.JavaConverters._

      val rows = List(
        createRow("2017-01-01T00:00:00Z", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
        createRow("2017-01-01T00:01:00Z", 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0)
      )

      val inputSrc = new CsvDataInputSource(10, URI.create("file:///tmp"), false)

      val inputStream = inputSrc.unpackRows(new DataStream(testEnv.fromCollection(rows.asJava)))

      val verifier = new OutputVerifier[DataPoint] {

        val points: ListBuffer[DataPoint] = ListBuffer[DataPoint]()

        override def receive(t: DataPoint): Unit = {
          points += t
        }

        override def init(): Unit = {}

        override def finish(): Unit = {
          points.size shouldEqual 20
          points.map(_.timestamp).toSet.size shouldEqual 2

          val seq = points.toIndexedSeq

          for (i <- 0 until seq.length - 1; j = i + 1) {
            seq(i).timestamp.compareTo(seq(j).timestamp) should be <= 0
          }

          for (i <- seq.indices) {

            inside(seq(i).sensor) {
              case Sensor(nStr) => if (i < 10) seq(i).value shouldEqual nStr.toDouble +- 1e-9 else
                seq(i).value shouldEqual nStr.toDouble * 10.0  +- 1e-9
            }
          }
        }
      }

      val sink = testEnv.createTestSink(verifier)

      inputStream.addSink(sink)

      testEnv.executeTest()
    }

  }

}

object CsvInputTest {

  def createRow(ts : String, values : Double*) : Row = {
    val row = new Row(values.length + 1)
    row.setField(0, ts)
    for (i <- values.indices) row.setField(i + 1, values(i))
    row
  }

  val Sensor: Regex = "Sensor-(\\d+)".r

  val CsvFile = "TestFile.csv"

}
