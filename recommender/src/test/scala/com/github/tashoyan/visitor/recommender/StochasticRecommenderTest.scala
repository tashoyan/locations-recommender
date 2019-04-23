package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.test.SparkTestHarness
import org.scalatest.FunSuite

class StochasticRecommenderTest extends FunSuite with SparkTestHarness {

  test("makeRecommendations") {
    val spark0 = spark
    import spark0.implicits._

    val stochasticEdges = Seq(
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
      .toDF("source_id", "target_id", "balanced_weight")

    val recommender = new StochasticRecommender(
      stochasticEdges,
      epsilon = 0.01,
      maxIterations = 20
    )
    val recommendations = recommender.makeRecommendations(vertexId = 1L, maxRecommendations = 10)
    println("Recommendations:")
    recommendations.foreach(println)
  }

}
