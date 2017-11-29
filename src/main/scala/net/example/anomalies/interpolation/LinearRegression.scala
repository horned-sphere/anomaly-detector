package net.example.anomalies.interpolation
import breeze.linalg.{DenseMatrix, DenseVector}
import net.example.anomalies.model._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.concurrent.duration.FiniteDuration

/**
  * Interpolation strategy that attempts to fill in gaps using linear regression. The strategy windows the data
  * and attempts to fill in data in the central region of the window by performing a linear regression over the
  * entire window. The slide of the window is set such that all points will be in the central region of exactly
  * one window. Additionally, a history of the most recently seen points, before the start of the window, is kept
  * to allow for linear extrapolation in the case where there is a long gap in the data. Extrapolation will be performed
  * until a configurable time after the last good data point.
  * @param windowSlide The slide of the windows.
  * @param paddingMultiple The multiple of the window slide on either side of the central region.
  * @param historyLen The length of history to attempt to keep for extrapolation.
  * @param maxGap The maximum time to extrapolate for.
  */
class LinearRegression(windowSlide : FiniteDuration, paddingMultiple : Int,
                       historyLen : Int, maxGap : FiniteDuration) extends InterpolationStrategy {

  require(paddingMultiple >= 1, s"Padding multiple must be at least 1: $paddingMultiple")
  require(historyLen >= 1, s"History length must be at least 1: $historyLen")

  private val windowLength = (2 * paddingMultiple + 1) * windowSlide

  import LinearRegression._

  override def interpolate(flagged: DataStream[FlaggedData]): DataStream[CorrectedDataPoint] = {

    val slidingWindow = SlidingEventTimeWindows.of(Time.milliseconds(windowLength.toMillis),
      Time.milliseconds(windowSlide.toMillis))

    flagged
      .assignAscendingTimestamps(_.epochTimeStamp)
      .keyBy(_.sensor)
      .window(slidingWindow)
      .apply(new LinRegressionWindowFunction(windowSlide, paddingMultiple, historyLen, maxGap))
  }
}

object LinearRegression {


  /**
    * Window function to apple the linear regression over the central region of the window.
    * @param windowSlide The slide of the windows.
    * @param paddingMultiple The multiple of the window slide on either side of the central region.
    * @param historyLen The length of history to attempt to keep for extrapolation.
    * @param maxGap The maximum time to extrapolate for.
    */
  class LinRegressionWindowFunction(windowSlide : FiniteDuration,
                                    paddingMultiple : Int,
                                    historyLen : Int, maxGap : FiniteDuration)
    extends RichWindowFunction[FlaggedData, CorrectedDataPoint, String, TimeWindow] {


    /**
      * State containing the history.
      */
    private var history : ListState[Good] = _

    override def open(parameters: Configuration): Unit = {
      history = getRuntimeContext.getListState(new ListStateDescriptor[Good](
        "LinearRegressionHistory", implicitly[TypeInformation[Good]]))
    }

    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[FlaggedData],
                       out: Collector[CorrectedDataPoint]): Unit = {
      import scala.collection.JavaConverters._

      val (newHist, outputs) = interpolate(window.getStart, window.getEnd,
        windowSlide.toMillis, paddingMultiple,
        history.get().asScala, historyLen, maxGap.toMillis,
        input)

      history.clear()
      for (entry <- newHist) history.add(entry)
      for (result <- outputs) out.collect(result)
    }
  }

  /**
    * Produce corrected data points for every point in the central region of the window.
    * @param windowStart The start time of the window.
    * @param windowEnd The end time of the window.
    * @param windowSlide The slide of the window.
    * @param paddingMultiple The padding multiple on either side of the centrel region.
    * @param history The history for extrapolation.
    * @param historyLen Target length of the history.
    * @param maxGap Maximum permitted extrapolation time.
    * @param data The window contents.
    * @return The new history and the corrected data.
    */
  def interpolate(windowStart : Long, windowEnd : Long,
                  windowSlide : Long, paddingMultiple : Int,
                  history : Iterable[Good], historyLen : Int, maxGap : Long,
                  data : Iterable[FlaggedData]) : (Iterable[Good], Iterable[CorrectedDataPoint]) = {

    //Determine where the central region lies.
    val centralMin = windowStart + paddingMultiple * windowSlide
    val centralMax = windowEnd - paddingMultiple * windowSlide

    //Find all of the good data.
    val good = for(rec @ Good(_, _, _, _, _) <- data) yield rec

    //Find all good data before the central region.
    val goodPrefix = good.filter(_.timestamp.toEpochMilli < centralMin)

    val centralRegion = data.filter(r =>
      r.epochTimeStamp >= centralMin && r.epochTimeStamp < centralMax)

    if (centralRegion.nonEmpty) {

      //Only attempt a fit if there are some bad points.
      val corrected = if (centralRegion.exists(r => r.isAnomalousOrMissing)) {

        val lastGoodBefore = (goodPrefix.lastOption, history.lastOption) match {
          case (Some(lastGood), _) => Some((Mode.Interpolate, lastGood))
          case (_, Some(lastHist)) => Some((Mode.Extrapolate, lastHist))
          case _ => None
        }

        val seed = (lastGoodBefore, centralRegion.head)

        //Determine which points require extrapolation and which interpolation.
        val withHandling = centralRegion.tail.scanLeft(seed) { (prev, r) =>
          r match {
            case good : Good => (Some((Mode.Interpolate, good)), good)
            case _ => (prev._1, r)
          }
        }

        //Create the extrapolation fit, where appropriate.
        val extrapolationFit = if (goodPrefix.isEmpty && history.size >= 2) {
          val histStart = history.head.timestamp.toEpochMilli
          val histEnd = history.last.timestamp.toEpochMilli
          fitFor(histStart, histEnd + maxGap, history)
        } else None

        //Create the interpolation fit, where possible.
        val interpolationFit = fitFor(windowStart, windowEnd, good)

        //Attempt to correct the central region.
        for ((goodBefore, record) <- withHandling) yield {
          (goodBefore, record) match {
            case (_, rec : Good) => CorrectedDataPoint(rec.id, rec.timestamp, Some(rec.value),
              rec.sensor, rec.score, PointStatus.Original)
            case (Some((mode, lastGood)), rec : Anomalous) if rec.epochTimeStamp - lastGood.epochTimeStamp <= maxGap =>
              val optFit = if (mode == Mode.Interpolate) interpolationFit else extrapolationFit
              optFit match {
                case Some(fit) => fit(rec)
                case _ => CorrectedDataPoint(record.id, record.timestamp, None,
                  record.sensor, rec.score, PointStatus.InterpolationFailure)
              }
            case _ => CorrectedDataPoint(record.id, record.timestamp, None, record.sensor,
              record.score, PointStatus.InterpolationFailure)
          }

        }
      } else {
        for (Good(id, timestamp, value, sensor, score) <- centralRegion) yield CorrectedDataPoint(
          id, timestamp,
          Some(value), sensor, Some(score), PointStatus.Original)
      }
      //Update the history.
      val newHistory = (history ++ goodPrefix).takeRight(historyLen)
      (newHistory, corrected)
    } else (history, Nil)
  }

  object Mode extends Enumeration {
    type Mode = Value
    val Extrapolate, Interpolate = Value
  }

  /**
    * Description of a linear fit.
    * @param min Minimum time of the interval for the fit.
    * @param max Maximum time of the interval for the fit.
    * @param gradient The gradient.
    * @param intercept The intercept.
    */
  case class LinearFit(min : Long, max: Long, gradient : Double, intercept : Double) {

    private val deltaT = (max - min).toDouble

    def apply(badPoint : Anomalous) : CorrectedDataPoint = {
      badPoint match {
        case Anomalous(id, timestamp, sensor, score) =>
          val x = (timestamp.toEpochMilli - min).toDouble / deltaT
          val replacement = gradient * x + intercept
          CorrectedDataPoint(id, timestamp, Some(replacement), sensor, score, PointStatus.Interpolated)
      }
    }

  }

  /**
    * Perform the linear regression on a sequence of data points.
    * @param minTime The minimum time of the fit.
    * @param maxTime The maximum time of the fit.
    * @param data The data points.
    * @return The fit, if it could be computed.
    */
  def fitFor(minTime : Long, maxTime : Long, data : Iterable[Good]) : Option[LinearFit] = {
    val observations = data.toIndexedSeq.map(r => (r.timestamp.toEpochMilli, r.value))
    if (observations.length < 2) {
      None
    } else {

      val deltaT = (maxTime - minTime).toDouble
      val n = observations.length

      val design = DenseMatrix.ones[Double](n, 2)
      val obs = DenseVector(observations.map(_._2).toArray)

      for (i <- 0 until n) design.update(i, 0, (observations(i)._1 - minTime).toDouble / deltaT)

      val solution = design \ obs

      val m = solution(0)
      val c = solution(1)
      Some(LinearFit(min = minTime, max = maxTime, gradient = m, intercept = c))
    }
  }

}
