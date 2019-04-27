package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object StochasticGraphBuilderMain extends StochasticGraphBuilderArgParser {

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

    val locationVisits = spark.read
      .parquet(s"${config.samplesDir}/location_visits_sample")
    val places = spark.read
      .parquet(s"${config.samplesDir}/places_sample")

    Console.out.println("Generating place visits")
    val placeVisits = PlaceVisits.calcPlaceVisits(locationVisits, places)
      .cache()
    printPlaceVisits(placeVisits)
    writePlaceVisits(placeVisits)

    generateRegionGraphs(placeVisits)
  }

  private def generateRegionGraphs(placeVisits: DataFrame)(implicit spark: SparkSession, config: StochasticGraphBuilderConfig): Unit = {
    import spark.implicits._

    val regionIds = placeVisits
      .select("region_id")
      .distinct()
      .as[Long]
      .collect()
      .toSeq

    val perRegionPlaceVisits = regionIds.
      map(regionId => (Seq(regionId), extractRegionsPlaceVisits(Seq(regionId), placeVisits)))
    val pairwiseRegionPlaceVisits = regionIds.combinations(2)
      .map(regIds => (regIds.sorted, extractRegionsPlaceVisits(regIds, placeVisits)))

    val regionStochasticGraphs = (perRegionPlaceVisits ++ pairwiseRegionPlaceVisits)
      .map { case (regIds, regPlaceVisits) =>
        (DataUtils.generateGraphFileName(regIds, config.samplesDir), generateStochasticGraph(regPlaceVisits))
      }
    regionStochasticGraphs.foreach { case (fileName, graph) =>
      Console.out.println(s"Writing stochastic graph : $fileName")
      writeStochasticGraph(fileName, graph)
    }
  }

  private def extractRegionsPlaceVisits(regionIds: Seq[Long], placeVisits: DataFrame): DataFrame = {
    val whereCondition: Column = regionIds
      .map(col("region_id") === _)
      .reduce(_ or _)
    placeVisits
      .where(whereCondition)
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
    graph.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
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
