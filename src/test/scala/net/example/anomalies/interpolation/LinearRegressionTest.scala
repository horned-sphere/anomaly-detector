package net.example.anomalies.interpolation

import java.time.Instant
import java.util.UUID

import net.example.anomalies.model._
import org.scalatest.{FunSpec, Matchers}

/**
  * Unit tests for [[LinearRegression]].
  */
class LinearRegressionTest extends FunSpec with Matchers {

  import LinearRegressionTest._

  describe("The linear regression interpolator") {

    it("should fill in single missing data points surrounded with good data") {

      val data = (0 to 6).map(n => p(n + 5, n.toDouble)) ++ IndexedSeq(bad(12)) ++ (8 to 15).map(n => p(n + 5, n.toDouble))

      val (history, interpolated) = LinearRegression.interpolate(5000, 20000, 5000, 1, Nil, 8, MaxGap, data)

      checkGood(interpolated, (5 until 10).map(n => (n + 5L, n.toDouble)))

      val histSeq = history.toIndexedSeq

      histSeq.length shouldEqual 5
      histSeq(0).timestamp.toEpochMilli shouldEqual 5000L
      histSeq.last.timestamp.toEpochMilli shouldEqual 9000L
    }

    it("should fill in successive missing data points surrounded with good data") {

      val data = (0 to 6).map(n => p(n + 5, n.toDouble)) ++ IndexedSeq(bad(12), bad(13)) ++
        (9 to 15).map(n => p(n + 5, n.toDouble))

      val (history, interpolated) = LinearRegression.interpolate(5000, 20000, 5000, 1, Nil, 8, MaxGap, data)

      checkGood(interpolated, (5 until 10).map(n => (n + 5L, n.toDouble)))

      val histSeq = history.toIndexedSeq

      histSeq.length shouldEqual 5
      histSeq(0).timestamp.toEpochMilli shouldEqual 5000L
      histSeq.last.timestamp.toEpochMilli shouldEqual 9000L
    }

    it("should fill in an entire missing central region surrounded with good data") {

      val data = (0 to 4).map(n => p(n + 5, n.toDouble)) ++ IndexedSeq(bad(10), bad(11), bad(12), bad(13), bad(14)) ++
        (10 to 15).map(n => p(n + 5, n.toDouble))

      val (history, interpolated) = LinearRegression.interpolate(5000, 20000, 5000, 1, Nil, 8, MaxGap, data)

      checkGood(interpolated, (5 until 10).map(n => (n + 5L, n.toDouble)))

      val histSeq = history.toIndexedSeq

      histSeq.length shouldEqual 5
      histSeq(0).timestamp.toEpochMilli shouldEqual 5000L
      histSeq.last.timestamp.toEpochMilli shouldEqual 9000L
    }

    it("should fail to provide estimates with no preceding observations") {
      val data = (0 to 9).map(n => bad(n + 5)) ++ (10 to 15).map(n => p(n + 5, n.toDouble))
      val (history, interpolated) = LinearRegression.interpolate(5000, 20000, 5000, 1, Nil, 8, MaxGap, data)

      interpolated.size shouldEqual 5
      for (rec <- interpolated) {
        rec.value shouldEqual None
        rec.status shouldEqual PointStatus.InterpolationFailure
      }

      history.isEmpty should be (true)
    }

    it("should extrapolate when adequate history is available") {
      val data = (0 to 9).map(n => bad(n + 5)) ++ (10 to 15).map(n => p(n + 5, n.toDouble))

      val oldHistory = List(Good(UUID.randomUUID(), Instant.ofEpochMilli(3000), -2.0, Sensor),
        Good(UUID.randomUUID(), Instant.ofEpochMilli(4000), -1.0, Sensor))

      val (history, interpolated) = LinearRegression.interpolate(5000, 20000, 5000, 1, oldHistory, 8, MaxGap, data)

      checkGood(interpolated, (5 until 10).map(n => (n + 5L, n.toDouble)))

      history.toList shouldEqual oldHistory
    }

    it("should stop extrapolating after the maximum gap has been exceeded") {

      val oldHistory = (0 to 7).map(n =>
        Good(UUID.randomUUID(), Instant.ofEpochMilli(n * 1000), n.toDouble, Sensor)).toList

      val data = (0 to 9).map(n => bad(n + 31)) ++ (10 to 15).map(n => p(n + 31, (n + 31).toDouble))

      val (history, interpolated) = LinearRegression.interpolate(31000, 46000, 5000, 1, oldHistory, 8, MaxGap, data)

      val intSeq = interpolated.toIndexedSeq

      interpolated.size shouldEqual 5
      intSeq(0).value.isDefined should be (true)
      intSeq(0).value.get shouldEqual 36.0 +- 1e-6
      intSeq(0).status shouldEqual PointStatus.Interpolated
      intSeq(1).value.isDefined should be (true)
      intSeq(1).value.get shouldEqual 37.0 +- 1e-6
      intSeq(1).status shouldEqual PointStatus.Interpolated

      for (i <- 2 until 5) {
        intSeq(i).value shouldEqual None
        intSeq(i).status shouldEqual PointStatus.InterpolationFailure
      }

      history.toList shouldEqual oldHistory
    }

  }

  /**
    * Check that an interpolated sequence is correct (where all values are expected to be filled in).
    * @param actual The actual sequence.
    * @param expected Expected timestamps and values.
    */
  def checkGood(actual : Iterable[CorrectedDataPoint], expected : IndexedSeq[(Long, Double)]) : Unit = {
    val actualSeq = actual.toIndexedSeq
    actualSeq.length shouldEqual expected.length

    for ((actRec, (ts, value)) <- actual.zip(expected)) {
      actRec.timestamp.toEpochMilli shouldEqual ts * 1000
      actRec.value.isDefined should be (true)
      actRec.value.get shouldEqual value +- 1e-6
      actRec.status should (be(PointStatus.Original) or be(PointStatus.Interpolated))
    }
  }

}

object LinearRegressionTest {

  /**
    * Maximum permitted gap (30 seconds).
    */
  val MaxGap = 30000

  /**
    * Sensor name.
    */
  val Sensor = "sensor"

  /**
    * Create a bad data point.
    * @param t The timestamp in seconds.
    * @return The point.
    */
  def bad(t : Long) : FlaggedData = Anomalous(UUID.randomUUID(),
    Instant.ofEpochMilli(t * 1000), Sensor, Some(1.0))

  /**
    * Create a good data point.
    * @param t The timestamp in seconds.
    * @param v The value.
    * @return The point.
    */
  def p(t : Long, v : Double) : FlaggedData = Good(UUID.randomUUID(),
    Instant.ofEpochMilli(t * 1000), v, Sensor)

}