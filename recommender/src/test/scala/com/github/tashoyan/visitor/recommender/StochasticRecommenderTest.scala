package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.test.SparkTestHarness
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.scalatest.Matchers._

//TODO Remove println
//TODO Tests on corner cases (zero epsilon, maxIterations)
class StochasticRecommenderTest extends FunSuite with SparkTestHarness {

  private val sample = Seq(
    (1L, 2L, 0.4),
    (1L, 3L, 0.24),
    (1L, 5L, 0.36),
    (2L, 4L, 0.3),
    (2L, 3L, 0.7),
    (3L, 5L, 1.0),
    (4L, 2L, 0.3),
    (4L, 5L, 0.7),
    (5L, 3L, 1.0)
  )

  assertSampleStochastic()

  private def assertSampleStochastic(): Unit = {
    val weightSums: Map[Long, Double] = sample
      .groupBy(_._1)
      .mapValues(_.map(_._3).sum)
    weightSums.foreach { case (vertexId, weightSum) =>
      assert(weightSum === 1.0, s"Sum of weights for vertex $vertexId must be 1.0")
    }
  }

  private def stochasticEdges(): DataFrame = {
    val spark0 = spark
    import spark0.implicits._
    sample.toDF("source_id", "target_id", "balanced_weight")
  }

  test("makeRecommendations - 1 iteration") {
    val spark0 = spark
    import spark0.implicits._

    val recommender = new StochasticRecommender(
      stochasticEdges(),
      epsilon = 0.01,
      maxIterations = 1
    )
    val recommendations = recommender
      .makeRecommendations(vertexId = 1L)
      .as[(Long, Double)]
      .collect()

    println("Recommendations:")
    recommendations.foreach(println)

    val expectedRecommendations = Seq(
      (5L, 0.3502),
      (3L, 0.3298),
      (2L, 0.11900000000000001),
      (4L, 0.051)
    )
    recommendations should be(expectedRecommendations)
  }

  test("makeRecommendations - converge") {
    val spark0 = spark
    import spark0.implicits._

    val recommender = new StochasticRecommender(
      stochasticEdges(),
      epsilon = 0.05,
      maxIterations = 1000
    )
    val recommendations = recommender
      .makeRecommendations(vertexId = 1L)
      .as[(Long, Double)]
      .collect()

    println("Recommendations:")
    recommendations.foreach(println)

    val expectedRecommendations = Seq(
      (3L, 0.408242766375),
      (5L, 0.3716171248749999),
      (2L, 0.055161925125),
      (4L, 0.014978183624999999)
    )
    recommendations should be(expectedRecommendations)
  }

  test("makeRecommendations - non-existing vertex") {
    val recommender = new StochasticRecommender(
      stochasticEdges(),
      epsilon = 0.05,
      maxIterations = 1000
    )
    intercept[IllegalArgumentException] {
      recommender.makeRecommendations(vertexId = 100L)
    }
  }

}
