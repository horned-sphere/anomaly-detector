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
    * @return Maximum gap in time over which interpolation will be attempted.
    */
  def maxDataGap : FiniteDuration

  /**
    * @return Configuration for the MAD outlier detection.
    */
  def outlierConfig : MadOutlierConfig

}

/**
  * Configuration parameters for the MAD outlier detection.
  */
trait MadOutlierConfig {

  /**
    * @return The length of the windows used for outlier detection.
    */
  def windowLength : FiniteDuration

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
