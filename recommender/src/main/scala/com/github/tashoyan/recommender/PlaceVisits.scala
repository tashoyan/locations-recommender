package com.github.tashoyan.recommender

import java.sql.Timestamp

import com.github.tashoyan.recommender.PlaceVisits._
import org.apache.spark.sql.functions.{col, max, min, udf}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

trait PlaceVisits {

  protected def calcPlaceVisits(locationVisits: DataFrame, places: DataFrame, lastDaysCount: Int)(implicit spark: SparkSession): DataFrame = {
    val visitsFrom = calcVisitsFromTimestamp(locationVisits, lastDaysCount)
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
      .where(col("timestamp") >= visitsFrom)
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
        col("id") as "place_id",
        col("category_id")
      )
    placeVisits
  }

  private def calcVisitsFromTimestamp(locationVisits: DataFrame, lastDaysCount: Int)(implicit spark: SparkSession): Timestamp = {
    import spark.implicits._

    val maxTimestamp = locationVisits
      .select(max(col("timestamp")))
      .as[Timestamp]
      .head()
    Timestamp.valueOf(
      maxTimestamp.toLocalDateTime
        .minusDays(lastDaysCount.toLong)
    )
  }

  protected def extractRegionsPlaceVisits(placeVisits: DataFrame, regionIds: Seq[Long]): Seq[(Seq[Long], DataFrame)] = {
    val regionIdsSeq: Seq[Seq[Long]] = regionIds.map(Seq(_)) ++
      regionIds.combinations(2)
    regionIdsSeq map extractRegionsPlaceVisits0(placeVisits)
  }

  protected def extractRegionIds(places: DataFrame)(implicit spark: SparkSession): Seq[Long] = {
    import spark.implicits._

    places
      .select("region_id")
      .distinct()
      .as[Long]
      .collect()
      .toSeq
  }

  private def extractRegionsPlaceVisits0(placeVisits: DataFrame)(regionIds: Seq[Long]): (Seq[Long], DataFrame) = {
    val whereCondition: Column = regionIds
      .map(col("region_id") === _)
      .reduce(_ or _)
    val regionsPlaceVisits = placeVisits
      .where(whereCondition)
    (regionIds, regionsPlaceVisits)
  }

  protected def printPlaceVisits(placeVisits: DataFrame): Unit = {
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

  protected def writePlaceVisits(placeVisits: DataFrame, dataDir: String): Unit = {
    placeVisits
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$dataDir/place_visits")
  }

}

object PlaceVisits {

  private val distanceAccuracyMeters: Double = 100

}
