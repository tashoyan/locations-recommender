package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object VisitGraphBuilder extends VisitGraphBuilderArgParser {

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

    val personLikesPlaceEdges = PersonLikesPlace.calcPersonLikesPlaceEdges(placeVisits)
    personLikesPlaceEdges.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/person_likes_place_edges")
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
