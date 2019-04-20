package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import VisitsGraph._

class VisitsGraph {

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

    //TODO How to partition input data sets? Both data sets on region id?
    //TODO How to partition the output data set?
    locationVisits
      .withColumnRenamed("latitude", "location_latitude")
      .withColumnRenamed("longitude", "location_longitude")
      .withColumnRenamed("region_id", "location_region_id")
      .join(
        places,
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
        col("id") as "place_id"
      )
  }

}

object VisitsGraph {
  val distanceAccuracyMeters: Double = 100
}
