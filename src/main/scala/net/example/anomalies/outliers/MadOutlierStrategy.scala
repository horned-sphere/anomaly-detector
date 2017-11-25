package net.example.anomalies.outliers

import breeze.linalg.DenseVector
import net.example.anomalies.model.{Anomalous, DataPoint, FlaggedData, Good}
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
  * @param windowLength The length of the sliding windows.
  * @param windowSlide The slide of the windows.
  * @param thresholdMultiple The threshold multiple for outlier flagging.
  */
class MadOutlierStrategy(windowLength: FiniteDuration,
                         windowSlide: FiniteDuration,
                         thresholdMultiple: Double) extends OutlierStrategy {

  import MadOutlierStrategy._

  require(windowSlide * 2 > windowLength,
    s"Window slide ($windowSlide) is less than half the window length ($windowLength).")

  require(thresholdMultiple > 0.0, s"Threshold multiple must be positive: $thresholdMultiple.")

  override def flagOutliers(in: DataStream[DataPoint]): DataStream[FlaggedData] = {

    val slidingWindow = SlidingEventTimeWindows.of(Time.milliseconds(windowLength.toMillis),
      Time.milliseconds(windowSlide.toMillis))

    in.keyBy(_.sensor)
      .window(slidingWindow)
      .apply(new MadWindowFunction(windowSlide, thresholdMultiple))

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
    * @param thresholdMultiple The threshold multiple for outlier flagging.
    */
  class MadWindowFunction(slide: FiniteDuration,
                          thresholdMultiple: Double) extends WindowFunction[DataPoint, FlaggedData, String, TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[DataPoint],
                       out: Collector[FlaggedData]): Unit = {
      for (rec <- outliers(window.getStart, window.getEnd, slide.toMillis, input, thresholdMultiple)) out.collect(rec)
    }
  }

  /**
    * Select outliers in the central region of a window. The central region is the part of the window that does not
    * intersect with neighbouring windows.
    * @param minTime The minimum time of the window.
    * @param maxTime The maximum time of the window.
    * @param slide The slide of the windows.
    * @param data The data in the window.
    * @param thresholdMultiple The threshold multiple for outlier flagging.
    * @return The flagged data from the central region.
    */
  def outliers(minTime: Long,
               maxTime: Long, slide: Long,
               data: Iterable[DataPoint],
               thresholdMultiple: Double): Iterable[FlaggedData] = {

    //Compute the median of all data in the window.
    val values = DenseVector(data.map(_.value).toArray)
    val windowMedian = median(values)

    //Compute the absolute deviation from the median.
    val absDeviations = values.map(x => Math.abs(x - windowMedian))

    val mad = median(absDeviations)

    //Find the central region bounds.
    val centralMin = maxTime - slide
    val centralMax = minTime + slide

    val scaledMad = mad * Consistency

    //Flag all records in the central region.
    for (record <- data; epoch = record.timestamp.toEpochMilli if centralMin <= epoch && epoch < centralMax) yield {
      val score = Math.abs(record.value - windowMedian) / scaledMad

      if (Math.abs(record.value - windowMedian) > thresholdMultiple) {
        Anomalous(record, score)
      } else {
        Good(record)
      }
    }
  }

}
