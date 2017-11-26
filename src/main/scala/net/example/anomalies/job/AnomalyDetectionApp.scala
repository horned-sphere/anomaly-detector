package net.example.anomalies.job

import java.io.File

import net.example.anomalies.config.{JobConfiguration, PropertiesConfiguration}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scopt.OptionParser

class AnomalyDetectionApp(jobName : String, config : JobConfiguration) extends Runnable {
  override def run() : Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(config.checkpointInterval.toMillis)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.minTimeBetweenCheckpoints.toMillis)

    env.execute(jobName)

  }
}

object AnomalyDetectionApp {

  def main(args : Array[String]) : Unit = {
    CmdLineParser.parse(args, Config()) match {
      case Some(conf) => new AnomalyDetectionApp(conf.jobName,
        PropertiesConfiguration(conf.configFile)).run()
      case _ =>

    }
  }

  val CmdLineParser: OptionParser[Config] = new OptionParser[Config]("anomaly-detector") {
    head("Anomaly Detector", "1.0")

    opt[String]('j', "jobName").action((name, c) => c.copy(jobName = name))
        .text("The name of the job")

    opt[File]('c', "configFile").required().valueName("<file>").
      action((f, c) => c.copy(configFile = f)).
      text("Path of the configuration file.")
  }
  case class Config(jobName : String = "Anomaly Detector", configFile : File = null)

}
