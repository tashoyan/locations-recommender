package com.github.tashoyan.recommender.stochastic

import com.github.tashoyan.recommender.{DataUtils, PlaceVisits}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StochasticGraphBuilderMain extends StochasticGraphBuilderArgParser with PlaceVisits {

  private val betaPlacePlace: Double = 1.0
  private val betaCategoryPlace: Double = 1.0

  def main(args: Array[String]): Unit = {
    parser.parse(args, StochasticGraphBuilderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: StochasticGraphBuilderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val locationVisits = DataUtils.loadLocationVisits(config.dataDir)
    val places = DataUtils.loadPlaces(config.dataDir)

    Console.out.println("Generating place visits")
    val placeVisits = calcPlaceVisits(locationVisits, places)
      .cache()
    printPlaceVisits(placeVisits)
    writePlaceVisits(placeVisits, config.dataDir)

    generateRegionGraphs(placeVisits, places)
  }

  private def generateRegionGraphs(placeVisits: DataFrame, places: DataFrame)(implicit spark: SparkSession, config: StochasticGraphBuilderConfig): Unit = {
    val regionIds = extractRegionIds(places)
    val regionsPlaceVisits = extractRegionsPlaceVisits(placeVisits, regionIds)

    regionsPlaceVisits.foreach { case (regIds, regPlaceVisits) =>
      val regGraph = generateStochasticGraph(regPlaceVisits)
      val graphFileName = DataUtils.generateGraphFileName(regIds, config.dataDir)
      writeStochasticGraph(graphFileName, regGraph)
    }
  }

  private def generateStochasticGraph(placeVisits: DataFrame)(implicit config: StochasticGraphBuilderConfig): DataFrame = {
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
      config.betaPersonPlace,
      config.betaPersonCategory
    )
    val stochasticGraph = StochasticGraphBuilder.buildWithBalancedWeights(betas, allEdges)
    stochasticGraph
  }

  private def writeStochasticGraph(fileName: String, graph: DataFrame): Unit = {
    Console.out.println(s"Writing stochastic graph : $fileName")
    graph.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

}
