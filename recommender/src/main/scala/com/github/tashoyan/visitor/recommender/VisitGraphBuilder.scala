package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.recommender.VisitGraphBuilder._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class VisitGraphBuilder(
    betaPlacePlace: Double,
    betaCategoryPlace: Double,
    betaPersonPlace: Double,
    betaPersonCategory: Double
) {

  def buildVisitGraph(placeVisits: DataFrame): DataFrame = {
    val placeSimilarPlaceEdges = PlaceSimilarPlace.calcPlaceSimilarPlaceEdges(placeVisits)
    val categorySelectedPlaceEdges = CategorySelectedPlace.calcCategorySelectedPlaceEdges(placeVisits)
    val personLikesPlaceEdges = PersonLikesPlace.calcPersonLikesPlaceEdges(placeVisits)
    val personLikesCategoryEdges = PersonLikesCategory.calcPersonLikesCategoryEdges(placeVisits)
    val betas = Seq(
      betaPlacePlace,
      betaCategoryPlace,
      betaPersonPlace,
      betaPersonCategory
    )
    val allEdges = Seq(
      placeSimilarPlaceEdges,
      categorySelectedPlaceEdges,
      personLikesPlaceEdges,
      personLikesCategoryEdges
    )
    composeWithBalancedWeights(betas, allEdges)
  }

}

object VisitGraphBuilder {

  //TODO Test: the graph is stochastic - for any vertex, the sum of outbound edges' weights is 1.0
  def composeWithBalancedWeights(betas: Seq[Double], allEdges: Seq[DataFrame]): DataFrame = {
    val firstBeta = betas.head
    val firstEdges = allEdges.head
    val firstGraph = firstEdges
      .select(
        col("source_id"),
        col("target_id"),
        col("weight") * firstBeta as "balanced_weight",
        col("region_id")
      )
    val otherBetas = betas.tail
    val otherEdges = allEdges.tail
    (otherEdges zip otherBetas).foldLeft(firstGraph) { case (graph, (edges, beta)) =>
      graph union
        edges
        .select(
          col("source_id"),
          col("target_id"),
          col("weight") * beta as "balanced_weight",
          col("region_id")
        )
    }
  }

}
