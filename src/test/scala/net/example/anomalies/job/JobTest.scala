package net.example.anomalies.job

import java.io.{File, FileInputStream}
import java.time.Instant
import java.util
import java.util.{Properties, UUID}

import net.example.anomalies.config.PropertiesConfiguration
import net.example.anomalies.io.CsvDataInputSource
import net.example.anomalies.model.DataPoint
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FunSpec, Matchers}
import org.apache.flink.api.scala._

class JobTest extends FunSpec with Matchers {

  describe("A job") {
    /*it ("should work") {

      val env = StreamExecutionEnvironment.createLocalEnvironment(1)

      val in = new FileInputStream("/Users/greg/interview/test.properties")

      val props = new Properties()

      props.load(in)

      val config = new PropertiesConfiguration(props)

      val job = new AnomalyDetectionApp("Job", new File("/Users/greg/interview/TestFile.csv").toURI, config)

      job.runWith(env)

    }*/
  }

}
