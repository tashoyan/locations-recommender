package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.test.SparkTestHarness
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.sum

class VisitGraphBuilderTest extends FunSuite with SparkTestHarness {

  test("composeWithBalancedWeights") {
    val spark0 = spark
    import spark0.implicits._

    //TODO Rename entities: Person, Place, Category
    val betas = Seq(
      0.4, //user - track
      0.6, //user - artist
      1.0, //artist - artist
      0.3, //track - track
      0.7 //track - artist
    )
    val userTrackEdges = Seq(
      (1, 2, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val userArtistEdges = Seq(
      (1, 3, 0.4, 0),
      (1, 5, 0.6, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val artistArtistEdges = Seq(
      (3, 5, 1.0, 0),
      (5, 3, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val trackTrackEdges = Seq(
      (2, 4, 1.0, 0),
      (4, 2, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")
    val trackArtistEdges = Seq(
      (2, 3, 1.0, 0),
      (4, 5, 1.0, 0)
    )
      .toDF("source_id", "target_id", "weight", "region_id")

    val allEdges = Seq(
      userTrackEdges,
      userArtistEdges,
      artistArtistEdges,
      trackTrackEdges,
      trackArtistEdges
    )
    val balancedWeightsGraph = VisitGraphBuilder.composeWithBalancedWeights(betas, allEdges)
    balancedWeightsGraph.show(false)
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
