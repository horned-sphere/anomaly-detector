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
                         thresholdMultiple: Double) extends OutlierStrategy {

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

  override def flagPoint(scored: ScoredPoint): FlaggedData = {
    if (scored.anomalyScore > thresholdMultiple) {
      Anomalous(scored.dataPoint, scored.anomalyScore)
    } else {
      Good(scored.dataPoint)
    }
  }
}

object MadOutlierStrategy {

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
    val values = DenseVector(data.map(_.value).toArray)
    val windowMedian = median(values)

    //Compute the absolute deviation from the median.
    val absDeviations = values.map(x => Math.abs(x - windowMedian))

    val mad = median(absDeviations)

    //Find the central region bounds.
    val centralMin = minTime + paddingMultiple * slide
    val centralMax = maxTime - paddingMultiple * slide

    val scaledMad = mad * Consistency

    //Flag all records in the central region.
    for (record <- data; epoch = record.timestamp.toEpochMilli if centralMin <= epoch && epoch < centralMax) yield {
      val score = Math.abs(record.value - windowMedian) / scaledMad

      ScoredPoint(record, score)
    }
  }

}
