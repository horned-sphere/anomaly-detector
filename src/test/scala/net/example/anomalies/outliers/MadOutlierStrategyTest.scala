package net.example.anomalies.outliers

import breeze.linalg.DenseVector
import org.scalatest.{FunSpec, Matchers}

/**
  * Tests for [[MadOutlierStrategy]].
  */
class MadOutlierStrategyTest extends FunSpec with Matchers {

  describe("The computation of the MAD") {

    it("should produce correct values on contrived data") {

      val (median, mad) = MadOutlierStrategy.computeMedianAndMad(
        new DenseVector[Double]((1 to 10).toArray.map(_.toDouble)))

      median shouldEqual 5.5 +- 1e-12
      mad shouldEqual 2.5 +- 1e-12
    }

  }

  describe("The selection of the central region of a window") {

    it("should select the correct points when none fall on the boundaries") {

      val central = MadOutlierStrategy.filterCentral(
        0, 15, 5, 1, List(0L, 2L, 4L, 6L, 8L, 11L, 12L, 14L, 15L)).toList
      central shouldEqual List(6L, 8L)
    }

    it("should include points on the lower boundary and exclude points on the upper") {
      val central = MadOutlierStrategy.filterCentral(
        0, 15, 5, 1, 0L to 15L).toList
      central shouldEqual (5L to 9L).toList
    }

  }

  describe("Scoring points against the MAD") {

    it("should give the deviation of the point by the Gaussian scaled MAD") {

      val withScores = MadOutlierStrategy.score(2.0, 2.5, List(3.0, 4.0)).toIndexedSeq

      withScores.length shouldEqual 2

      withScores(0)._1 shouldEqual 3.0
      withScores(0)._2 shouldEqual 1.0 / (4.0 * MadOutlierStrategy.Consistency) +- 1e-12

      withScores(1)._1 shouldEqual 4.0
      withScores(1)._2 shouldEqual 3.0 / (4.0 * MadOutlierStrategy.Consistency) +- 1e-12


    }

  }

}
