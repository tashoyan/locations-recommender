package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, row_number, first, lit}

class SimilarPersonsBuilder(alphaPlace: Double, alphaCategory: Double, kNearest: Int) {

  private val visitedPlacesTopN: Int = 100
  private val visitedCategoriesTopN: Int = 100

  def calcSimilarPersons(placeVisits: DataFrame): DataFrame = {
    val placeRatings = calcPlaceRatings(placeVisits)
    val placeBasedVectors = calcPlaceBasedVectors(placeRatings)
    val placeBasedSimilarities = calcPlaceBasedSimilarities(placeBasedVectors)

    val categoryRatings = calcCategoryRatings(placeVisits)
    val categoryBasedVectors = calcCategoryBasedVectors(categoryRatings)
    val categoryBasedSimilarities = calcCategoryBasedSimilarities(categoryBasedVectors)

    val similarities = calcSimilarities(placeBasedSimilarities, categoryBasedSimilarities)
    keepKNearest(similarities)
  }

  private def calcPlaceRatings(placeVisits: DataFrame): DataFrame = {
    val personVisitPlaceCounts = placeVisits
      .groupBy("person_id", "place_id")
      .agg(count("*") as "place_rating")

    val window = Window.partitionBy("person_id")
      .orderBy(col("place_rating").desc)
    personVisitPlaceCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= visitedPlacesTopN)
      .drop("rank")
  }

  private def calcPlaceBasedVectors(placeRatings: DataFrame): DataFrame = {
    placeRatings
      .groupBy("person_id")
      //TODO Implement
      .agg(first("place_rating") as "place_rating_vector")
  }

  private def calcPlaceBasedSimilarities(placeBasedVectors: DataFrame): DataFrame = {
    val thatVectors = placeBasedVectors
      .withColumnRenamed("person_id", "that_person_id")
    (placeBasedVectors crossJoin thatVectors)
      //TODO Implement
      .withColumn("place_based_similarity", lit(0.5))
      .where(col("place_based_similarity") > 0)
      .select(
        "person_id",
        "that_person_id",
        "place_based_similarity"
      )
  }

  private def calcCategoryRatings(placeVisits: DataFrame): DataFrame = {
    val personVisitCategoryCounts = placeVisits
      .groupBy("person_id", "category_id")
      .agg(count("*") as "category_rating")

    val window = Window.partitionBy("person_id")
      .orderBy(col("category_rating").desc)
    personVisitCategoryCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= visitedCategoriesTopN)
      .drop("rank")
  }

  private def calcCategoryBasedVectors(categoryRatings: DataFrame): DataFrame = {
    categoryRatings
      .groupBy("person_id")
      //TODO Implement
      .agg(first("category_rating") as "category_rating_vector")
  }

  private def calcCategoryBasedSimilarities(categoryBasedVectors: DataFrame): DataFrame = {
    val thatVectors = categoryBasedVectors
      .withColumnRenamed("person_id", "that_person_id")
    (categoryBasedVectors crossJoin thatVectors)
      //TODO Implement
      .withColumn("category_based_similarity", lit(0.5))
      .where(col("category_based_similarity") > 0)
      .select(
        "person_id",
        "that_person_id",
        "category_based_similarity"
      )
  }

  private def calcSimilarities(placeBasedSimilarities: DataFrame, categoryBasedSimilarities: DataFrame): DataFrame = {
    placeBasedSimilarities
      .join(categoryBasedSimilarities, Seq("person_id", "that_person_id"))
      .select(
        col("person_id"),
        col("that_person_id"),
        col("place_based_similarity") * alphaPlace +
          col("category_based_similarity") * alphaCategory
          as "similarity"
      )
  }

  private def keepKNearest(similarities: DataFrame): DataFrame = {
    val window = Window.partitionBy("person_id")
      .orderBy(col("similarity").desc)
    similarities
      .withColumn("rn", row_number() over window)
      .where(col("rn") <= kNearest)
      .drop("rn")
  }

}
