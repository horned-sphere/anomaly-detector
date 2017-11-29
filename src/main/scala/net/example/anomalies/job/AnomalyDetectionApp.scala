package net.example.anomalies.job

import java.io.File
import java.net.{InetAddress, InetSocketAddress, URI}

import net.example.anomalies.config.{JobConfiguration, PropertiesConfiguration}
import net.example.anomalies.interpolation.LinearRegression
import net.example.anomalies.io.{CorrectedPointOutput, CsvDataInputSource, DataPointOutput, ScoredPointOutput}
import net.example.anomalies.model.{CorrectedDataPoint, DataPoint, ScoredPoint}
import net.example.anomalies.outliers.MadOutlierStrategy
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import scopt.OptionParser

/**
  * Anomaly detection job entry point.
  * @param jobName The name of the job.
  * @param dataPath The path to the input data.
  * @param extraSpaces Whether the input CSV has extra spaces.
  * @param config The job configuration.
  */
class AnomalyDetectionApp(jobName : String,
                          dataPath : URI,
                          extraSpaces : Boolean,
                          config : JobConfiguration) extends Runnable {
  override def run() : Unit = {

    //Configure Flink.
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    runWith(env)

  }

  def runWith(env: StreamExecutionEnvironment): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(config.checkpointInterval.toMillis)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(config.minTimeBetweenCheckpoints.toMillis)

    //Get the input data.
    val input = new CsvDataInputSource(config.numSensors, dataPath, extraSpaces)

    val inputSrc = input.loadSource(env)

    //Configure the outlier strategy and the interpolation strategy.

    val outlierStrat = new MadOutlierStrategy(1, config.outlierConfig.windowSlide, config.outlierConfig.thresholdMultiple)

    val interpStrat = new LinearRegression(config.interpolationConfig.windowSlide, 1,
      config.interpolationConfig.historyLength,
      config.interpolationConfig.maximumGap)

    //Configure the elasticsearch outputs.

    val esConfig = new java.util.HashMap[String, String]
    esConfig.put("cluster.name", config.elasticSearch.clusterName)
    esConfig.put("bulk.flush.max.actions", "1")

    val esParams = config.elasticSearch

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(
      InetAddress.getByName(esParams.hostIp), esParams.port))

    val target = esParams.outputTarget

    val directSink = new ElasticsearchSink[DataPoint](esConfig, transportAddresses,
      new DataPointOutput(target.indexName, target.typeName))

    val scoredSink = new ElasticsearchSink[ScoredPoint](esConfig, transportAddresses,
      new ScoredPointOutput(target.indexName, target.typeName))

    val correctedSink = new ElasticsearchSink[CorrectedDataPoint](esConfig, transportAddresses,
      new CorrectedPointOutput(target.indexName, target.typeName))

    //Wire up the job.
    val job = new AnomalyDetectionJob(outlierStrat, interpStrat, directSink, scoredSink, correctedSink)

    job.transform(inputSrc)

    env.execute(jobName)
  }
}

object AnomalyDetectionApp {

  def main(args : Array[String]) : Unit = {
    CmdLineParser.parse(args, Config()) match {
      case Some(conf) => new AnomalyDetectionApp(conf.jobName, conf.dataPath, conf.extraSpaces,
        PropertiesConfiguration(conf.configFile)).run()
      case _ =>

    }
  }

  /**
    * Command line parameter parser.
    */
  val CmdLineParser: OptionParser[Config] = new OptionParser[Config]("anomaly-detector") {
    head("Anomaly Detector", "1.0")

    opt[String]('j', "jobName").action((name, c) => c.copy(jobName = name))
        .text("The name of the job")

    opt[File]('c', "configFile").required().valueName("<file>").
      action((f, c) => c.copy(configFile = f)).
      text("Path of the configuration file.")

    opt[URI]('p', "data").action((path, c) => c.copy(dataPath = path))
      .required()
      .valueName("<uri>")
      .text("Input data URI.")

    opt[Unit]("extraSpaces")
        .action((_, c) => c.copy(extraSpaces = true))
        .text("Whether the CSV file has extra spaces.")
  }

  /**
    * Job configuration type.
    * @param jobName The name of the job.
    * @param dataPath The path to the input data.
    * @param configFile The path to the configuration file.
    * @param extraSpaces Whether the CSV file has extra spaces.
    */
  case class Config(jobName : String = "Anomaly Detector",
                    dataPath : URI = null,
                    configFile : File = null,
                    extraSpaces : Boolean = false)

}
