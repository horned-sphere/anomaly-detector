package net.example.anomalies.job

import net.example.anomalies.interpolation.InterpolationStrategy
import net.example.anomalies.model.{CorrectedDataPoint, DataPoint, FlaggedData, ScoredPoint}
import net.example.anomalies.outliers.OutlierStrategy
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
  * The main anomaly detection workflow.
  * @param outlierStrategy Strategy to select outliers.
  * @param interpolation Strategy to replace flagged points.
  * @param directOut Output for the raw data.
  * @param scoredOut Output for the scored data.
  * @param correctedOut Output for the corrected data.
  */
class AnomalyDetectionJob(outlierStrategy: OutlierStrategy, interpolation : InterpolationStrategy,
                          directOut : SinkFunction[DataPoint],
                          scoredOut : SinkFunction[ScoredPoint],
                          correctedOut : SinkFunction[CorrectedDataPoint]) {

  import AnomalyDetectionJob._

  /**
    * Pipe the input to the outputs.
    * @param input The input data.
    * @return The outputs.
    */
  def transform(input : DataStream[DataPoint])
    : (DataStreamSink[DataPoint], DataStreamSink[ScoredPoint], DataStreamSink[CorrectedDataPoint]) = {

    val splitInput = input.split(_ => List(Raw, Scored))

    val scored = outlierStrategy.scoreData(splitInput.select(Scored))
        .process(new FlaggedSplitter(outlierStrategy.flagPoint))

    val flagged = scored.getSideOutput(FlaggedOutputTag)

    val corrected = interpolation.interpolate(flagged)

    (splitInput.select(Raw).addSink(directOut), scored.addSink(scoredOut), corrected.addSink(correctedOut))
  }


}

object AnomalyDetectionJob {

  /**
    * Name of the raw data path.
    */
  final val Raw = "RawOutput"

  /**
    * Name of the scored data path.
    */
  final val Scored = "ScoredOutput"

  /**
    * Flagged data side channel.
    */
  val FlaggedOutputTag: OutputTag[FlaggedData] = OutputTag[FlaggedData]("flaggedData")

  /**
    * Splits of the flagged data from the scored data.
    * @param flagger Flag operation.
    */
  class FlaggedSplitter(flagger : ScoredPoint => FlaggedData) extends ProcessFunction[ScoredPoint, ScoredPoint] {
    override def processElement(value: ScoredPoint,
                                ctx: ProcessFunction[ScoredPoint, ScoredPoint]#Context,
                                out: Collector[ScoredPoint]): Unit = {
      out.collect(value)

      ctx.output(FlaggedOutputTag, flagger(value))
    }
  }

}
