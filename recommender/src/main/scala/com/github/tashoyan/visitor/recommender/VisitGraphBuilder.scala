package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object VisitGraphBuilder extends VisitGraphBuilderArgParser {

  val betaPlacePlace: Double = 1.0
  val betaCategoryPlace: Double = 1.0
  val betaPersonPlace: Double = 0.5
  val betaPersonCategory: Double = 0.5

  def main(args: Array[String]): Unit = {
    parser.parse(args, VisitGraphBuilderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: VisitGraphBuilderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    val locationVisits = spark.read
      .parquet(s"${config.samplesDir}/location_visits_sample")
    val places = spark.read
      .parquet(s"${config.samplesDir}/places_sample")

    val placeVisits = PlaceVisits.calcPlaceVisits(locationVisits, places)
      .cache()
    //    printPlaceVisits(placeVisits)
    writePlaceVisits(placeVisits)

    val placeSimilarPlaceEdges = PlaceSimilarPlace.calcPlaceSimilarPlaceEdges(placeVisits)
    placeSimilarPlaceEdges.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/place_similar_place_edges")

    val categorySelectedPlaceEdges = CategorySelectedPlace.calcCategorySelectedPlaceEdges(placeVisits)
    categorySelectedPlaceEdges.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/category_selected_place_edges")

    val personLikesPlaceEdges = PersonLikesPlace.calcPersonLikesPlaceEdges(placeVisits)
    personLikesPlaceEdges.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/person_likes_place_edges")

    val personLikesCategoryEdges = PersonLikesCategory.calcPersonLikesCategoryEdges(placeVisits)
    personLikesCategoryEdges.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/person_likes_category_edges")

    val visitGraph = buildGraph(
      placeSimilarPlaceEdges,
      categorySelectedPlaceEdges,
      personLikesPlaceEdges,
      personLikesCategoryEdges
    )
    visitGraph.write
      .partitionBy("region_id")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/visit_graph")
  }

  //TODO Test: the graph is stochastic - for any vertex, the sum of outbound edges' weights is 1.0
  private def buildGraph(
      placeSimilarPlaceEdges: DataFrame,
      categorySelectedPlaceEdges: DataFrame,
      personLikesPlaceEdges: DataFrame,
      personLikesCategoryEdges: DataFrame
  ): DataFrame = {
    placeSimilarPlaceEdges
      .select(
        col("source_id"),
        col("target_id"),
        col("weight") * betaPlacePlace as "balanced_weight",
        col("region_id")
      ) union
        categorySelectedPlaceEdges
        .select(
          col("source_id"),
          col("target_id"),
          col("weight") * betaCategoryPlace as "balanced_weight",
          col("region_id")
        ) union
          personLikesPlaceEdges
          .select(
            col("source_id"),
            col("target_id"),
            col("weight") * betaPersonPlace as "balanced_weight",
            col("region_id")
          ) union
            personLikesCategoryEdges
            .select(
              col("source_id"),
              col("target_id"),
              col("weight") * betaPersonCategory as "balanced_weight",
              col("region_id")
            )
  }

  def printPlaceVisits(placeVisits: DataFrame): Unit = {
    println(s"Place visits count: ${placeVisits.count()}")
    placeVisits
      .select(min("timestamp"), max("timestamp"))
      .show(false)
    println("Place visits counts by region:")
    placeVisits
      .groupBy("region_id")
      .count()
      .show(false)
    val visitorsCount = placeVisits
      .select("person_id")
      .distinct()
      .count()
    println(s"Visitors total count: $visitorsCount")
    val topN = 10
    println(s"Top $topN visitors:")
    placeVisits
      .groupBy("person_id")
      .count()
      .orderBy(col("count").desc)
      .limit(topN)
      .show(false)
    println("Place visits sample:")
    placeVisits.show(false)
  }

  private def writePlaceVisits(placeVisits: DataFrame)(implicit config: VisitGraphBuilderConfig): Unit = {
    placeVisits
      .write
      .partitionBy("region_id", "year_month")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/place_visits")
  }

}
