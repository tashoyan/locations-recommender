package com.github.tashoyan.visitor.recommender.stochastic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum}
import org.apache.spark.sql.types.DoubleType

object PersonLikesPlace {

  private val visitedPlacesTopN: Int = 100

  def calcPersonLikesPlaceEdges(placeVisits: DataFrame): DataFrame = {
    val personVisitPlaceCounts = placeVisits
      .groupBy("person_id", "place_id")
      .agg(count("*") as "visit_count")

    val window = Window.partitionBy("person_id")
      .orderBy(col("visit_count").desc)
    val topVisitedPlaces = personVisitPlaceCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= visitedPlacesTopN)
      .drop("rank")
      .cache()

    val totalVisitedPlaces = topVisitedPlaces
      .groupBy("person_id")
      .agg(sum("visit_count") as "total_visit_count")

    topVisitedPlaces
      .join(totalVisitedPlaces, "person_id")
      .withColumn("weight", col("visit_count") / col("total_visit_count") cast DoubleType)
      .select(
        col("person_id") as "source_id",
        col("place_id") as "target_id",
        col("weight")
      )
  }

}
