package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StochasticGraphBuilderMain extends StochasticGraphBuilderArgParser {

  val betaPlacePlace: Double = 1.0
  val betaCategoryPlace: Double = 1.0
  val betaPersonPlace: Double = 0.5
  val betaPersonCategory: Double = 0.5

  def main(args: Array[String]): Unit = {
    parser.parse(args, StochasticGraphBuilderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: StochasticGraphBuilderConfig): Unit = {
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
    val categorySelectedPlaceEdges = CategorySelectedPlace.calcCategorySelectedPlaceEdges(placeVisits)
    val personLikesPlaceEdges = PersonLikesPlace.calcPersonLikesPlaceEdges(placeVisits)
    val personLikesCategoryEdges = PersonLikesCategory.calcPersonLikesCategoryEdges(placeVisits)
    val allEdges = Seq(
      placeSimilarPlaceEdges,
      categorySelectedPlaceEdges,
      personLikesPlaceEdges,
      personLikesCategoryEdges
    )
    val betas = Seq(
      betaPlacePlace,
      betaCategoryPlace,
      betaPersonPlace,
      betaPersonCategory
    )
    val stochasticGraph = StochasticGraphBuilder.buildWithBalancedWeights(betas, allEdges)
    writeStochasticGraph(stochasticGraph)
  }

  private def writeStochasticGraph(graph: DataFrame)(implicit config: StochasticGraphBuilderConfig): Unit = {
    graph.write
      .partitionBy("region_id")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/stochastic_graph")
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

  def writePlaceVisits(placeVisits: DataFrame)(implicit config: StochasticGraphBuilderConfig): Unit = {
    placeVisits
      .write
      .partitionBy("region_id", "year_month")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/place_visits")
  }

}
