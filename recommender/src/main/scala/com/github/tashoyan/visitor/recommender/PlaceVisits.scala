package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PlaceVisits {

  val distanceAccuracyMeters: Double = 100

  def calcPlaceVisits(locationVisits: DataFrame, places: DataFrame): DataFrame = {
    val placeIsVisitedUdf = udf { (
      locationLatitude: Double,
      locationLongitude: Double,
      placeLatitude: Double,
      placeLongitude: Double
    ) =>
      val location = Location(locationLatitude, locationLongitude)
      val place = Location(placeLatitude, placeLongitude)
      (location distanceMeters place) <= distanceAccuracyMeters
    }

    val placeVisits = locationVisits
      .withColumnRenamed("latitude", "location_latitude")
      .withColumnRenamed("longitude", "location_longitude")
      .join(places, "region_id")
      .where(
        placeIsVisitedUdf(
          col("location_latitude"),
          col("location_longitude"),
          col("latitude"),
          col("longitude")
        )
      )
      .select(
        col("person_id"),
        col("timestamp"),
        col("year_month"),
        col("region_id"),
        col("id") as "place_id",
        col("category_id")
      )
    placeVisits
      .repartition(col("region_id"), col("year_month"))
  }

}
