package com.github.tashoyan.visitor.recommender.stochastic

import com.github.tashoyan.visitor.test.SparkTestHarness
import org.apache.spark.sql.functions.sum
import org.scalatest.FunSuite

class StochasticGraphBuilderTest extends FunSuite with SparkTestHarness {

  test("buildWithBalancedWeights") {
    val spark0 = spark
    import spark0.implicits._

    val betas = Seq(
      0.4, //person - place
      0.6, //person - category
      1.0, //category - category
      0.3, //place - place
      0.7 //place - category
    )
    val personPlaceEdges = Seq(
      (1, 2, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val personCategoryEdges = Seq(
      (1, 3, 0.4, 0),
      (1, 5, 0.6, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val categoryCategoryEdges = Seq(
      (3, 5, 1.0, 0),
      (5, 3, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val placePlaceEdges = Seq(
      (2, 4, 1.0, 0),
      (4, 2, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val placeCategoryEdges = Seq(
      (2, 3, 1.0, 0),
      (4, 5, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")

    val allEdges = Seq(
      personPlaceEdges,
      personCategoryEdges,
      categoryCategoryEdges,
      placePlaceEdges,
      placeCategoryEdges
    )
    val balancedWeightsGraph = StochasticGraphBuilder.buildWithBalancedWeights(betas, allEdges)
    val balancedWeightSums = balancedWeightsGraph
      .groupBy("source_id")
      .agg(sum("balanced_weight") as "sum")
      .as[(Long, Double)]
      .collect()
    balancedWeightSums.foreach { case (sourceId, sum) =>
      assert(sum === 1.0, s"For source $sourceId sum must be 1.0")
    }
  }

}
