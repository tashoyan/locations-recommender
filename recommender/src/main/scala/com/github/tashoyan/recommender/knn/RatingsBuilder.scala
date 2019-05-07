package com.github.tashoyan.recommender.knn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank}

object RatingsBuilder {

  private val visitedPlacesTopN: Int = 100
  private val visitedCategoriesTopN: Int = 10

  @deprecated(message = "don't use", since = "now")
  def calcPlaceRatings(placeVisits: DataFrame): DataFrame = {
    calcRatings(
      placeVisits,
      entityIdColumn = "place_id",
      ratingColumn = "place_rating",
      topN = visitedPlacesTopN
    )
  }

  @deprecated(message = "don't use", since = "now")
  def calcCategoryRatings(placeVisits: DataFrame): DataFrame = {
    calcRatings(
      placeVisits,
      entityIdColumn = "category_id",
      ratingColumn = "category_rating",
      topN = visitedCategoriesTopN
    )
  }

  def calcRatings(
      placeVisits: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      topN: Int
  ): DataFrame = {
    val personVisitCounts = placeVisits
      .groupBy("person_id", entityIdColumn)
      .agg(count("*") as ratingColumn)

    val window = Window.partitionBy("person_id")
      .orderBy(col(ratingColumn).desc)
    personVisitCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= topN)
      .drop("rank")
  }

}
