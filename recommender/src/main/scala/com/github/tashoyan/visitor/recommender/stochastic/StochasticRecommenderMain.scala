package com.github.tashoyan.visitor.recommender.stochastic

import com.github.tashoyan.visitor.recommender.DataUtils
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object StochasticRecommenderMain extends StochasticRecommenderArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, StochasticRecommenderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  def getHomeRegionId(personId: Long, persons: DataFrame)(implicit spark: SparkSession): Try[Long] = {
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

  private def doMain(implicit config: StochasticRecommenderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val personsFile = s"${config.samplesDir}/persons_sample"
    Console.out.println(s"Loading persons from $personsFile")
    val persons = spark.read
      .parquet(personsFile)
      .withColumn("home_region_id", col("home_region_id") cast LongType)
    val placesFile = s"${config.samplesDir}/places_sample"
    Console.out.println(s"Loading places from $placesFile")
    val places = spark.read
      .parquet(placesFile)
      .withColumn("region_id", col("region_id") cast LongType)

    Console.out.println(
      """Enter ID of the person to be provided with recommendation and ID of the target region:
        | <person ID>[ <region ID>]
        |If region ID is omitted, then the home region of the person will be used.
      """.stripMargin
    )

    while (true) {
      val input = Try(
        StdIn.readLine("(CTRL-C for exit) <person ID>[ <region ID>]: ")
      )

      val parsedInput = input map parseInput
      val tryRecommenderTarget = parsedInput flatMap calcRecommenderTarget(persons)
      val tryRecommendations = tryRecommenderTarget flatMap makeRecommendations

      tryRecommendations match {
        case Success(recommendations) =>
          printRecommendations(tryRecommenderTarget.get, recommendations, places)
        case Failure(e) =>
          Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
  }

  private val inputRegex = """(\d+)\s*(\d+)?""".r

  private def parseInput(input: String): (Long, Option[Long]) = {
    input match {
      case inputRegex(personIdStr, regionIdStr) =>
        (personIdStr.toLong, Option(regionIdStr).map(_.toLong))
      case _ =>
        throw new IllegalArgumentException(s"Failed to parse input: $input")
    }
  }

  private def calcRecommenderTarget(persons: DataFrame)(personIdInputRegionId: (Long, Option[Long]))(implicit spark: SparkSession): Try[RecommenderTarget] = {
    val personId = personIdInputRegionId._1
    val inputRegionId = personIdInputRegionId._2
    val tryHomeRegionId = getHomeRegionId(personId, persons)
    tryHomeRegionId map calcRecommenderTarget(personId, inputRegionId)
  }

  private def calcRecommenderTarget(personId: Long, inputRegionId: Option[Long])(homeRegionId: Long): RecommenderTarget = {
    val targetRegionId = inputRegionId.getOrElse {
      Console.out.println("Target region ID is not provided - falling back to the person's home region")
      homeRegionId
    }
    val graphRegionIds =
      if (targetRegionId == homeRegionId)
        Seq(homeRegionId)
      else
        Seq(homeRegionId, targetRegionId)
    RecommenderTarget(personId, targetRegionId, graphRegionIds)
  }

  private def makeRecommendations(recommenderTarget: RecommenderTarget)(implicit spark: SparkSession, config: StochasticRecommenderConfig): Try[DataFrame] = {
    val stochasticGraph = loadStochasticGraph(recommenderTarget.graphRegionIds)
    val recommender = new StochasticRecommender(
      stochasticGraph,
      config.epsilon,
      config.maxIterations
    )
    Try(recommender.makeRecommendations(recommenderTarget.personId))
  }

  private def printRecommendations(
      recommenderTarget: RecommenderTarget,
      recommendations: DataFrame,
      places: DataFrame
  )(implicit config: StochasticRecommenderConfig): Unit = {
    val targetRegionId = recommenderTarget.targetRegionId
    val targetRegionPlaces = places
      .where(col("region_id") === targetRegionId)
    val recommendedPlaces = targetRegionPlaces
      .join(broadcast(recommendations), "id")
      .orderBy(col("probability").desc)
      .limit(config.maxRecommendations)

    Console.out.println(s"Person ${recommenderTarget.personId} might want to visit in region $targetRegionId:")
    recommendedPlaces.show(false)
  }

  private def loadStochasticGraph(regionIds: Seq[Long])(implicit spark: SparkSession, config: StochasticRecommenderConfig): DataFrame = {
    val stochasticGraphFile = DataUtils.generateGraphFileName(regionIds, config.samplesDir)
    Console.out.println(s"Loading stochastic graph of visited places from $stochasticGraphFile")
    val stochasticGraph = spark.read
      .parquet(stochasticGraphFile)
    stochasticGraph
  }

  case class RecommenderTarget(
      personId: Long,
      targetRegionId: Long,
      graphRegionIds: Seq[Long]
  )

}
