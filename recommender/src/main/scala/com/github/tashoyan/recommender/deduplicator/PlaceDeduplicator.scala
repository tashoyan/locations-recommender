package com.github.tashoyan.recommender.deduplicator

import com.github.tashoyan.recommender.Location
import com.github.tashoyan.recommender.deduplicator.Levenshtein._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class PlaceDeduplicator(
    maxPlaceDistanceMeters: Double,
    maxNameDifference: Int
) {

  def dropDuplicates(places: DataFrame, confirmedPlaces: DataFrame): DataFrame = {
    val thatConfirmedPlaces = confirmedPlaces
      .select(
        col("region_id"),
        col("id") as "that_id",
        col("name") as "that_name",
        col("latitude") as "that_latitude",
        col("longitude") as "that_longitude"
      )

    val maxPlaceDistanceMeters0 = maxPlaceDistanceMeters
    val maxNameDifference0 = maxNameDifference
    val isNotSamePlaceUdf = udf { (latitude: Double,
      longitude: Double,
      name: String,
      thatLatitude: Double,
      thatLongitude: Double,
      thatName: String) =>
      val placeLocation = Location(latitude, longitude)
      val thatPlaceLocation = Location(thatLatitude, thatLongitude)

      (placeLocation distanceMeters thatPlaceLocation) > maxPlaceDistanceMeters0 ||
        lev(name.toLowerCase, thatName.toLowerCase) > maxNameDifference0
    }

    val noDuplicatePlaces = places
      .join(thatConfirmedPlaces, "region_id")
      .where(col("id") =!= col("that_id"))
      .where(
        isNotSamePlaceUdf(
          col("latitude"),
          col("longitude"),
          col("name"),
          col("that_latitude"),
          col("that_longitude"),
          col("that_name")
        )
      )

    val columns = places.columns.map(col)
    noDuplicatePlaces.select(columns: _*)
  }

}
