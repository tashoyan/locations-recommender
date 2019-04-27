package com.github.tashoyan.visitor.recommender.stochastic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum}

object CategorySelectedPlace {

  val selectedPlacesTopN: Int = 100

  def calcCategorySelectedPlaceEdges(placeVisits: DataFrame): DataFrame = {
    val placeCounts = placeVisits
      .groupBy("category_id", "place_id")
      .agg(count("*") as "place_count")

    val window = Window.partitionBy("category_id")
      .orderBy(col("place_count").desc)
    val topSelectedPlaces = placeCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= selectedPlacesTopN)
      .drop("rank")
      .cache()

    val totalSelectedPlaces = topSelectedPlaces
      .groupBy("category_id")
      .agg(sum("place_count") as "total_place_count")

    topSelectedPlaces
      .join(totalSelectedPlaces, "category_id")
      .withColumn("weight", col("place_count") / col("total_place_count") cast DoubleType)
      .select(
        col("category_id") as "source_id",
        col("place_id") as "target_id",
        col("weight")
      )
  }

}
