package net.example.anomalies.config

import java.io.{File, FileInputStream, IOException}
import java.util.Properties

import scala.concurrent.duration.{Duration, FiniteDuration, _}
import scala.util.{Failure, Success, Try}

/**
  * Implementation of [[JobConfiguration]] backed by Java properties.
  * @param props The properties.
  */
class PropertiesConfiguration(props : Properties) extends JobConfiguration {

  import PropertiesConfiguration._

  override def checkpointInterval: FiniteDuration = getFiniteDuration(props, CHECKPOINT_INTERVAL, 15.minutes)

  override def minTimeBetweenCheckpoints: FiniteDuration = getFiniteDuration(props, CHECKPOINT_MIN_BETWEEN, 5.minutes)

  override def maxDataGap: FiniteDuration = getFiniteDuration(props, MAX_GAP, 15.minutes)

  override def outlierConfig : MadOutlierConfig = new MadOutlierConfig {

    override def windowLength: FiniteDuration = getFiniteDuration(props, MAD_WINDOW_LENGTH, 1.minute)

    override def thresholdMultiple: Double = getDouble(props, MAD_THRESHOLD, 4.0)

    override def windowSlide: FiniteDuration = getFiniteDuration(props, MAD_WINDOW_SLIDE, 40.seconds)
  }
}

object PropertiesConfiguration {

  //Property names.
  val CHECKPOINT_INTERVAL = "checkpoint.interval"
  val CHECKPOINT_MIN_BETWEEN = "checkpoint.min_between"
  val ASYNC_CHECKPOINTS = "checkpoints.do_async"
  val MAX_GAP = "interpolation.max_gap"
  val MAD_WINDOW_LENGTH = "outliers.window_length"
  val MAD_WINDOW_SLIDE = "outliers.window_slide"
  val MAD_THRESHOLD = "outliers.threshold_multiple"

  /**
    * Get a finite duration from a property.
    * @param props The properties.
    * @param key The name of the property.
    * @param default The default value.
    * @return The duration.
    */
  def getFiniteDuration(props : Properties, key : String, default : FiniteDuration) : FiniteDuration = {
    val value = props.getProperty(key)
    if (value == null) {
      default
    } else {
      Try(Duration(value)) match {
        case Success(Duration.Zero) => throw new IllegalArgumentException("Duration must be non-zero.")
        case Success(dur : FiniteDuration) => dur
        case Success(dur) => throw new IllegalArgumentException(s"$dur is not finite.")
        case Failure(t) => throw new IllegalArgumentException(s"$value is not a valid duration.", t)
      }
    }
  }

  /**
    * Get a boolean value from a property.
    * @param props The properties.
    * @param key The name of the property.
    * @param default The default value.
    * @return The boolean.
    */
  def getBoolean(props : Properties, key : String, default : Boolean) : Boolean = {
    val value = props.getProperty(key)
    if (value == null) {
      default
    } else {
      Try(value.toBoolean) match {
        case Success(p) => p
        case Failure(t) => throw new IllegalArgumentException(s"$value is not a valid boolean.", t)
      }
    }
  }

  /**
    * Get a double value from a property.
    * @param props The properties.
    * @param key The name of the property.
    * @param default The default value.
    * @return The double.
    */
  def getDouble(props : Properties, key : String, default : Double) : Double = {
    val value = props.getProperty(key)
    if (value == null) {
      default
    } else {
      Try(value.toDouble) match {
        case Success(x) => x
        case Failure(t) => throw new IllegalArgumentException(
          s"$value is not a valid double precision floating point number.", t)
      }
    }
  }

  /**
    * Load the configuration from a file.
    * @param file The file.
    * @return The configuration.
    */
  def apply(file : File) : PropertiesConfiguration = {
    val props = new Properties()
    val in = new FileInputStream(file)
    try {
      props.load(in)
      new PropertiesConfiguration(props)
    } catch{
      case t : IOException => throw new IllegalArgumentException(s"Failed to load configuration from $file.", t)
    } finally {
      in.close()
    }
  }

}
