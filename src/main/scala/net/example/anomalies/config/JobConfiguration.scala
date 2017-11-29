package net.example.anomalies.config

import scala.concurrent.duration.FiniteDuration

/**
  * Interface to describe the configuration required by the job.
  */
trait JobConfiguration extends Serializable {

  /**
    * @return The interval between state checkpoints.
    */
  def checkpointInterval : FiniteDuration

  /**
    * @return Minimum time that must have elapsed between successive checkpoints.
    */
  def minTimeBetweenCheckpoints : FiniteDuration

  /**
    * @return Configuration for the MAD outlier detection.
    */
  def outlierConfig : MadOutlierConfig

  /**
    * @return Configuration for the linear regression interpolator.
    */
  def interpolationConfig : LinearRegressionConfig

  /**
    * @return Number of sensors in the input.
    */
  def numSensors : Int

  /**
    * @return Elastic search configuration.
    */
  def elasticSearch : ElasticsearchConfig

}

/**
  * Configuration parameters for the MAD outlier detection.
  */
trait MadOutlierConfig {


  /**
    * @return The amount by which successive windows slide.
    */
  def windowSlide : FiniteDuration

  /**
    * @return Assuming noise on the observations is Gaussian, the multiple of the observed standard deviation
    *         (estimated by MAD) beyond which a point is considered an outlier.
    */
  def thresholdMultiple : Double

}

/**
  * Linear regression configuration.
  */
trait LinearRegressionConfig {

  /**
    * @return The window slide for interpolation.
    */
  def windowSlide : FiniteDuration

  /**
    * @return Maximum extrapolation time.
    */
  def maximumGap : FiniteDuration

  /**
    * @return Target length for the history for the purpose of extrapolation.
    */
  def historyLength : Int

}

/**
  * Elasticsearch index and type.
  */
trait ElasticSearchTarget {

  def indexName : String

  def typeName : String

}

/**
  * Elasticsearch configuration.
  */
trait ElasticsearchConfig {

  /**
    * @return The cluster name.
    */
  def clusterName : String

  /**
    * @return The host IP.
    */
  def hostIp : String

  /**
    * @return The port.
    */
  def port : Int

  /**
    * @return Output target for the raw data.
    */
  def directTarget : ElasticSearchTarget

  /**
    * @return Output target for the anomaly scores.
    */
  def scoredTarget : ElasticSearchTarget

  /**
    * @return Output target or the corrected data.
    */
  def correctedTarget : ElasticSearchTarget

}