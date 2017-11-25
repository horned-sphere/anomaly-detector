package net.example.anomalies.interpolation
import java.time.Instant

import net.example.anomalies.model._
import org.apache.flink.streaming.api.scala.DataStream

import scala.concurrent.duration.FiniteDuration
import org.apache.flink.api.scala._

/**
  * Attempts to fill in gaps using linear interpolation. For any gap less a configured maximum, linear interpolation
  * will be performed using the two points at either end of the gap. Anomalous points at the ends of data streams will
  * not be replaced.
  * @param maxGap The maximum permissible gap.
  */
class LinearInterpolation(maxGap : FiniteDuration) extends InterpolationStrategy {

  import LinearInterpolation._

  override def interpolate(flagged: DataStream[FlaggedData]) : DataStream[CorrectedDataPoint] = {
    flagged.keyBy(_.sensor)
      .flatMapWithState(interpStep(maxGap))
  }
}

object LinearInterpolation {


  /**
    * Interpolation state.
    * @param before The last good point.
    * @param missing The times for which we need to generate interpolated results.
    */
  case class InterpolationState(before : Option[DataPoint] = None,
                                missing : List[Instant] = Nil)

  /**
    * Update the interpolation state with a new date point.
    * @param maxGap The maximum permissible gap.
    * @param data The next data point.
    * @param optState The state, if initialized.
    * @return Data for the output stream that as computed during this step and the new value of the state.
    */
  def interpStep(maxGap : FiniteDuration)(data : FlaggedData, optState : Option[InterpolationState])
    : (List[CorrectedDataPoint], Option[InterpolationState]) ={

    val state = optState.getOrElse(InterpolationState())
    val (outputPoints, newState) = (state.before, data) match {

      case (None, Good(point)) => (List(goodPoint(point)), state.copy(before = Some(point)))

      case (None, Anomalous(point, _)) => (List(noInterpolation(point.timestamp, point.sensor)), state)

      case (Some(left), Anomalous(point, _)) =>

        //If we have more bad data, check against the maximum length.

        if (point.timestamp.toEpochMilli - left.timestamp.toEpochMilli > maxGap.toMillis) {
          //The maximum length is exceeded to we can give up on everything in the interpolation queue.
          ((point.timestamp :: state.missing).map(t => lacuna(t, point.sensor)), state.copy(missing = Nil))
        } else {
          (Nil, state.copy(missing = point.timestamp :: state.missing))
        }

      case (Some(left), Good(point)) =>
        //We've found another good point so can perform the interpolation and reset the state.
        (goodPoint(point) :: doInterpolation(left, state.missing, point), InterpolationState(Some(point), Nil))
    }

    (outputPoints.reverse, Some(newState))
  }

  /**
    * Perform the interpolation.
    * @param left Last point before the missing data.
    * @param missing The timestamps of the missing data.
    * @param right First point after the missing data.
    * @return The interpolated results.
    */
  def doInterpolation(left : DataPoint, missing : List[Instant], right : DataPoint) : List[CorrectedDataPoint] = {
    if (missing.nonEmpty) {
      val origin = left.timestamp.toEpochMilli
      val deltaT = (right.timestamp.toEpochMilli - origin).toDouble

      val leftVal = left.value

      val deltaVal = right.value - left.value

      missing.map { t =>
        val offset = (t.toEpochMilli - origin).toDouble
        val interpolated = leftVal + (offset / deltaT) * deltaVal
        CorrectedDataPoint(t, Some(interpolated), left.sensor, PointStatus.Interpolated)
      }
    } else Nil
  }

  /**
    * Packge a good point.
    * @param point The point.
    * @return The corrected point.
    */
  def goodPoint(point : DataPoint) : CorrectedDataPoint = CorrectedDataPoint(point.timestamp,
    Some(point.value), point.sensor, PointStatus.Original)

  /**
    * Package a point that could not be interpolated.
    * @param ts The timestamp.
    * @param sensor The name of the sensor.
    * @return The corrected point.
    */
  def noInterpolation(ts : Instant, sensor : String) : CorrectedDataPoint =
    CorrectedDataPoint(ts, None, sensor, PointStatus.InterpolationFailure)


  /**
    * Package a point that was not interpolated as it was too far from a good data point.
    * @param ts The timestamp.
    * @param sensor The name of the sensor.
    * @return The corrected point.
    */
  def lacuna(ts : Instant, sensor : String) : CorrectedDataPoint =
    CorrectedDataPoint(ts, None, sensor, PointStatus.DataLacuna)
}
