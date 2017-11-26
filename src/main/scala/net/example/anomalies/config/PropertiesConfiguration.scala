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

  override def outlierConfig : MadOutlierConfig = new MadOutlierConfig {

    override def thresholdMultiple: Double = getDouble(props, MAD_THRESHOLD, 4.0)

    override def windowSlide: FiniteDuration = getFiniteDuration(props, MAD_WINDOW_SLIDE, 40.seconds)
  }

  override def interpolationConfig : LinearRegressionConfig = new LinearRegressionConfig {

    override def maximumGap: FiniteDuration = getFiniteDuration(props, MAX_GAP, 15.minutes)

    override def historyLength: Int = getInt(props, HISTORY_LEN, 10)

    override def windowSlide: FiniteDuration = getFiniteDuration(props, INT_WINDOW_SLIDE, 5.minutes)
  }

  override def numSensors: Int = getInt(props, NUM_SENSORS, 10)

  override def elasticSearch : ElasticsearchConfig= new ElasticsearchConfig {

    override def hostIp: String = getString(props, ES_HOST_IP, "127.0.0.1")

    override def port: Int = getInt(props, ES_PORT, 9300)


    override def clusterName: String = getString(props, ES_CLUSTER_NAME, "my_cluster")

    override def directTarget: ElasticSearchTarget = new ElasticSearchTarget {

      override def indexName: String = getString(props, esIndexName("direct"), "rawData")

      override def typeName: String = getString(props, esTypeName("direct"), "raw")
    }
    override def scoredTarget: ElasticSearchTarget = new ElasticSearchTarget {

      override def indexName: String = getString(props, esIndexName("scored"), "scoredData")

      override def typeName: String = getString(props, esTypeName("scored"), "scored")
    }

    override def correctedTarget: ElasticSearchTarget = new ElasticSearchTarget {

      override def indexName: String = getString(props, esIndexName("corrected"), "correctedData")

      override def typeName: String = getString(props, esTypeName("corrected"), "corrected")
    }
  }
}

object PropertiesConfiguration {

  //Property names.
  val CHECKPOINT_INTERVAL = "checkpoint.interval"
  val CHECKPOINT_MIN_BETWEEN = "checkpoint.min_between"
  val ASYNC_CHECKPOINTS = "checkpoints.do_async"
  val MAX_GAP = "interpolation.max_gap"
  val HISTORY_LEN = "interpolation.history_len"
  val INT_WINDOW_SLIDE = "interpolation.window_slide"
  val MAD_WINDOW_SLIDE = "outliers.window_slide"
  val MAD_THRESHOLD = "outliers.threshold_multiple"
  val NUM_SENSORS = "num_sensors"
  val ES_HOST_IP = "es.host_ip"
  val ES_PORT = "es.port"
  val ES_CLUSTER_NAME = "es.cluster_name"
  def esIndexName(target : String) : String = s"es.$target.index"
  def esTypeName(target : String) : String = s"es.$target.type_name"

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
    * Get a string value from a property.
    * @param props The properties.
    * @param key The name of the property.
    * @param default The default value.
    * @return The string.
    */
  def getString(props : Properties, key : String, default : String) : String = {
    val value = props.getProperty(key)
    if (value == null) {
      default
    } else {
      value
    }
  }

  /**
    * Get an integer value from a property.
    * @param props The properties.
    * @param key The name of the property.
    * @param default The default value.
    * @return The integer.
    */
  def getInt(props : Properties, key : String, default : Int) : Int = {
    val value = props.getProperty(key)
    if (value == null) {
      default
    } else {
      Try(value.toInt) match {
        case Success(n) => n
        case Failure(t) => throw new IllegalArgumentException(
          s"$value is not a valid integer.", t)
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
