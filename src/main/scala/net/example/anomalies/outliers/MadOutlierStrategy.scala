package net.example.anomalies.outliers

import breeze.linalg.DenseVector
import net.example.anomalies.model._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import breeze.stats._
import org.apache.flink.api.scala._

import scala.concurrent.duration.FiniteDuration

/**
  * Outlier detection strategy which compares the deviation of the value of a point against the median
  * deviation from the median over a sliding window. Outliers are flagged based on a configurable threshold.
  * @param paddingMultiple The window length is (2 * paddingMultiple + 1) times the slide.
  * @param windowSlide The slide of the windows.
  * @param thresholdMultiple The threshold multiple for outlier flagging.
  */
class MadOutlierStrategy(paddingMultiple: Int,
                         windowSlide: FiniteDuration,
                         thresholdMultiple: Double) extends OutlierStrategy with Serializable {

  require(paddingMultiple >= 1, s"Padding multiple must be at least 1: $paddingMultiple")

  import MadOutlierStrategy._

  private val windowLength = (2 * paddingMultiple + 1) * windowSlide

  require(thresholdMultiple > 0.0, s"Threshold multiple must be positive: $thresholdMultiple.")

  override def scoreData(in: DataStream[DataPoint]): DataStream[ScoredPoint] = {

    val slidingWindow = SlidingEventTimeWindows.of(Time.milliseconds(windowLength.toMillis),
      Time.milliseconds(windowSlide.toMillis))

    in.keyBy(_.sensor)
      .window(slidingWindow)
      .apply(new MadWindowFunction(windowSlide, paddingMultiple, thresholdMultiple))

  }

  override def flagPoint = new Flagger(thresholdMultiple)
}

object MadOutlierStrategy {

  class Flagger(thresholdMultiple : Double) extends (ScoredPoint => FlaggedData) with Serializable {
    override def apply(scored: ScoredPoint): FlaggedData = {
      scored.dataPoint.value match {
        case Some(value) =>
          if (scored.anomalyScore > thresholdMultiple) {
          Anomalous(scored.dataPoint.id, scored.dataPoint.timestamp, scored.dataPoint.sensor, Some(scored.anomalyScore))
        } else {
          Good(scored.dataPoint.id, scored.dataPoint.timestamp, value, scored.dataPoint.sensor, scored.anomalyScore)
        }
        case _ => Anomalous(scored.dataPoint.id, scored.dataPoint.timestamp, scored.dataPoint.sensor, None)
      }

    }
  }

  /**
    * Default consistency scaling level (for Gaussian distributed data, this scales the MAD to be an estimator
    * of the standard deviation).
    */
  val Consistency = 1.4826

  /**
    * Window function to flag outliers.
    * @param slide The slide of the windows.
    * @param paddingMultiple The window length is (2 * paddingMultiple + 1) times the slide.
    * @param thresholdMultiple The threshold multiple for outlier flagging.
    */
  class MadWindowFunction(slide: FiniteDuration,
                          paddingMultiple: Int,
                          thresholdMultiple: Double) extends WindowFunction[DataPoint, ScoredPoint, String, TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[DataPoint],
                       out: Collector[ScoredPoint]): Unit = {
      for (rec <- score(window.getStart, window.getEnd, slide.toMillis,
        paddingMultiple, input, thresholdMultiple)) out.collect(rec)
    }
  }

  /**
    * Score data in the central region of a window. The central region is the part of the window that does not
    * intersect with neighbouring windows.
    * @param minTime The minimum time of the window.
    * @param maxTime The maximum time of the window.
    * @param slide The slide of the windows.
    * @param data The data in the window.
    * @param thresholdMultiple The threshold multiple for outlier flagging.
    * @return The flagged data from the central region.
    */
  def score(minTime: Long,
               maxTime: Long, slide: Long,
               paddingMultiple : Int,
               data: Iterable[DataPoint],
               thresholdMultiple: Double): Iterable[ScoredPoint] = {

    //Compute the median of all data in the window.
    val values = DenseVector(data.flatMap(_.value).toArray)
    val (windowMedian, mad) = computeMedianAndMad(values)

    val central: Iterable[DataPoint] = filterCentral(minTime, maxTime, slide, paddingMultiple, data)

    score(mad, windowMedian, central).map { case (record, score) => ScoredPoint(record, score) }
  }

  /**
    * Score the selected data.
    * @param mad The median absolute deviation.
    * @param windowMedian The window median.
    * @param central The region to score.
    */
  def score[T](mad : Double,
            windowMedian : Double,
            central : Iterable[T])(implicit m : Measurement[T]): Iterable[(T, Double)] = {
    val scaledMad = mad * Consistency

    //Flag all records in the central region.
    for (record <- central) yield {
      val score = m.getValue(record) match {
        case Some(r) => Math.abs(r - windowMedian) / scaledMad
        case _ => 0.0
      }

      (record, score)
    }
  }

  /**
    * Select the central region of the window.
    * @param minTime Minimum time of the window.
    * @param maxTime Maximum time of the window.
    * @param slide The window slide.
    * @param paddingMultiple The padding multiple of the slide.
    * @param data The data.
    * @return The central section.
    */
  private[outliers] def filterCentral[T](minTime: Long, maxTime: Long,
                    slide: Long, paddingMultiple: Int,
                    data: Iterable[T])(implicit t : Timestamped[T]): Iterable[T] = {
    //Find the central region bounds.
    val centralMin = minTime + paddingMultiple * slide
    val centralMax = maxTime - paddingMultiple * slide
    val central = data.filter { record =>
      val epoch = t.getTimestamp(record)
      centralMin <= epoch && epoch < centralMax
    }
    central
  }

  /**
    * Compute the median and median absolute deviation.
    *
    * @param values The values.
    * @return The MAD.
    */
  def computeMedianAndMad(values: DenseVector[Double]): (Double, Double) = {
    val windowMedian = median(values)

    //Compute the absolute deviations from the median.
    val absDeviations = values.map(x => Math.abs(x - windowMedian))

    val mad = median(absDeviations)
    (windowMedian, mad)
  }
}
