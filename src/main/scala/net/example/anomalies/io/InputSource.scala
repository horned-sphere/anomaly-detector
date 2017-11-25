package net.example.anomalies.io

import net.example.anomalies.model.DataPoint
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Interface for input sources for the detector.
  */
trait InputSource {

  /**
    * Load the data as a stream.
    * @param env The execution environment.
    * @return The stream.
    */
  def loadSource(env : StreamExecutionEnvironment) : DataStream[DataPoint]

}
