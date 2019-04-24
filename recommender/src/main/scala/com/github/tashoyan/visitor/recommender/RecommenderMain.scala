package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn
import scala.util.{Failure, Success, Try}

//TODO Refactor
//scalastyle:off
object RecommenderMain extends RecommenderArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, RecommenderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  def getHomeRegion(personId: Long, persons: DataFrame)(implicit spark: SparkSession): Try[Long] = {
    import spark.implicits._

    val regionIds = persons
      .where(col("id") === personId)
      .limit(1)
      .select("home_region_id")
      .as[Long]
      .collect()
    Try(
      regionIds
        .headOption
        .getOrElse(throw new NoSuchElementException(s"Person not found: $personId"))
    )
  }

  private def doMain(implicit config: RecommenderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val personsFile = s"${config.samplesDir}/persons_sample"
    Console.out.println(s"Loading persons from $personsFile")
    val persons = spark.read
      .parquet(personsFile)

    while (true) {
      val personIdInput = Try(
        StdIn.readLine("Enter ID of the person to be provided with recommendations (CTRL-C for exit):")
      )
      val targetRegionIdInput = Try(
        StdIn.readLine("Enter ID of the target region, leave blank for the person's home region (CTRL-C for exit):")
      )
      val input = for {
        personId <- personIdInput.map(_.toLong)
        regionIds <- calcRegionIdsForRecommender(personId, targetRegionIdInput, persons)
      } yield (personId, regionIds)
      input match {
        case Success((personId, regionIds)) =>
          printRecommendations(personId, regionIds)
        case Failure(e) =>
          Console.err.println("Failed to parse the input:")
          Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
  }

  private def calcRegionIdsForRecommender(personId: Long, regionIdInput: Try[String], persons: DataFrame)(implicit spark: SparkSession, config: RecommenderConfig): Try[Seq[Long]] = {
    val homeRegion = getHomeRegion(personId, persons)
    val emptyStrRegex = """\s*""".r
    regionIdInput.flatMap {
      case emptyStrRegex() =>
        Console.out.println("Target region is not provided; will take the person's home region")
        homeRegion
          .map(Seq(_))
      case regionIdStr =>
        homeRegion
          .map(Seq(_, regionIdStr.toLong))
    }
  }

  private def printRecommendations(personId: Long, regionIds: Seq[Long])(implicit spark: SparkSession, config: RecommenderConfig): Unit = {
    val stochasticGraph = loadStochasticGraph(regionIds)
    val recommender = new StochasticRecommender(
      stochasticGraph,
      config.epsilon,
      config.maxIterations
    )
    printRecommendations(personId, recommender)
  }

  private def loadStochasticGraph(regionIds: Seq[Long])(implicit spark: SparkSession, config: RecommenderConfig): DataFrame = {
    val stochasticGraphFile = generateGraphFileName(regionIds)
    Console.out.println(s"Loading stochastic graph of visited places from $stochasticGraphFile")
    val stochasticGraph = spark.read
      .parquet(stochasticGraphFile)
    stochasticGraph
  }

  //TODO Extract to a common module and remove the duplication with StochasticGraphBuilderMain
  private def generateGraphFileName(regionIds: Seq[Long])(implicit config: RecommenderConfig): String = {
    regionIds
      .map(regId => s"region$regId")
      .mkString(s"${config.samplesDir}/stochastic_graph_", "_", "")
  }

  private def printRecommendations(personId: Long, recommender: StochasticRecommender)(implicit spark: SparkSession, config: RecommenderConfig): Unit = {
    Try(recommender.makeRecommendations(personId, config.maxRecommendations)) match {
      case Success(recommendations) =>
        printRecommendations(personId, recommendations)
      case Failure(e) =>
        Console.err.println(s"Failed to get recommendations for person: $personId")
        Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
    }
  }

  private def printRecommendations(personId: Long, recommendations: DataFrame)(implicit spark: SparkSession, config: RecommenderConfig): Unit = {
    val placesFile = s"${config.samplesDir}/places_sample"
    Console.out.println(s"Loading places from $placesFile")
    val places = spark.read
      .parquet(placesFile)

    val recommendedPlaces = places
      .join(broadcast(recommendations), "id")
    Console.out.println(s"Person $personId might want to visit:")
    recommendedPlaces
      .orderBy(col("probability").desc)
      .show(false)
  }

}
