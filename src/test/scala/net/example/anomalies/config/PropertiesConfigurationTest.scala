package net.example.anomalies.config

import java.util.Properties

import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.duration._

/**
  * Tests for [[PropertiesConfiguration]].
  */
class PropertiesConfigurationTest extends FunSpec with Matchers {

  describe("The properties backed configuration"){

    it("should be populated correctly from an appropriate properties file") {

      val in = classOf[PropertiesConfigurationTest].getResourceAsStream("test.properties")

      in shouldNot be (null)

      val props = new Properties()
      try {
        props.load(in)
      } finally {
        in.close()
      }

      val conf = new PropertiesConfiguration(props)

      conf.checkpointInterval shouldEqual 30.minutes
      conf.minTimeBetweenCheckpoints shouldEqual 5.minutes
      conf.interpolationConfig.maximumGap shouldEqual 15.minutes
      conf.interpolationConfig.historyLength shouldEqual 8
      conf.interpolationConfig.windowSlide shouldEqual 1.minute
      conf.numSensors shouldEqual 10
      conf.outlierConfig.thresholdMultiple shouldEqual 4.0
      conf.outlierConfig.windowSlide shouldEqual 3.minutes
      conf.elasticSearch.hostIp shouldEqual "127.0.0.1"
      conf.elasticSearch.port shouldEqual 9300
      conf.elasticSearch.clusterName shouldEqual "test-cluster"
      conf.elasticSearch.directTarget.indexName shouldEqual "rawData"
      conf.elasticSearch.directTarget.typeName shouldEqual "measurement"
      conf.elasticSearch.scoredTarget.indexName shouldEqual "anomalies"
      conf.elasticSearch.scoredTarget.typeName shouldEqual "score"
      conf.elasticSearch.correctedTarget.indexName shouldEqual "correctedData"
      conf.elasticSearch.correctedTarget.typeName shouldEqual "measurement"
    }

  }

}
