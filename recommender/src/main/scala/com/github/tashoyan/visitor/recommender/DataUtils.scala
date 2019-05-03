package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

object DataUtils {

  def loadPersons(dataDir: String)(implicit spark: SparkSession): DataFrame = {
    val dataFile = s"$dataDir/persons_sample"
    Console.out.println(s"Loading persons from $dataFile")
    spark.read
      .parquet(dataFile)
      .withColumn("home_region_id", col("home_region_id") cast LongType)
  }

  def loadPlaces(dataDir: String)(implicit spark: SparkSession): DataFrame = {
    val dataFile = s"$dataDir/places_sample"
    Console.out.println(s"Loading places from $dataFile")
    spark.read
      .parquet(dataFile)
      .withColumn("region_id", col("region_id") cast LongType)
  }

  def loadLocationVisits(dataDir: String)(implicit spark: SparkSession): DataFrame = {
    val dataFile = s"$dataDir/location_visits_sample"
    Console.out.println(s"Loading location visits from $dataFile")
    spark.read
      .parquet(dataFile)
      .withColumn("region_id", col("region_id") cast LongType)
  }

  def generateGraphFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "stochastic_graph")
  }

  def generateSimilarPersonsFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "similar_persons")
  }

  def generatePlaceRatingsFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "place_ratings")
  }

  private def generateFileName(regionIds: Seq[Long], dirPath: String, filePrefix: String): String = {
    regionIds
      .sorted
      .distinct
      .map(regId => s"region$regId")
      .mkString(s"$dirPath/${filePrefix}_", "_", "")
  }

}
