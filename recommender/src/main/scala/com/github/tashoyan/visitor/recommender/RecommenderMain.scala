package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object RecommenderMain extends RecommenderArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, RecommenderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: RecommenderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val placesFile = s"${config.samplesDir}/places_sample"
    Console.out.println(s"Loading places from $placesFile")
    val places = spark.read
      .parquet(placesFile)

    val stochasticGraphFile = s"${config.samplesDir}/stochastic_graph"
    Console.out.println(s"Loading stochastic graph of visited places from $stochasticGraphFile")
    val stochasticGraph = spark.read
      .parquet(stochasticGraphFile)

    val recommender = new StochasticRecommender(
      stochasticGraph,
      config.epsilon,
      config.maxIterations
    )
    while (true) {
      val userInput = StdIn.readLine("Enter ID of the person to be provided with recommendations (CTRL-C for exit):")
      Try(userInput.toLong) match {
        case Success(personId) =>
          printRecommendations(personId, recommender, places)
        case Failure(e) =>
          Console.err.println(s"Failed to parse the input: $userInput")
          Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
  }

  private def printRecommendations(personId: Long, recommender: StochasticRecommender, places: DataFrame)(implicit config: RecommenderConfig): Unit = {
    Try(recommender.makeRecommendations(personId, config.maxRecommendations)) match {
      case Success(recommendations) =>
        printRecommendations(personId, recommendations, places)
      case Failure(e) =>
        Console.err.println(s"Failed to get recommendations for person: $personId")
        Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
    }
  }

  private def printRecommendations(personId: Long, recommendations: DataFrame, places: DataFrame): Unit = {
    val recommendedPlaces = places
      .join(broadcast(recommendations), "id")
    Console.out.println(s"Person $personId might want to visit:")
    recommendedPlaces
      .orderBy(col("probability").desc)
      .show(false)
  }

}
