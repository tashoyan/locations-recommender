package com.github.tashoyan.visitor.sample

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, Year, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class LocationVisitsSampleGenerator(
    regions: Seq[Region],
    personCount: Long,
    minPersonId: Long
)(implicit val config: SampleGeneratorConfig) extends Serializable {
  private val regionCount: Int = regions.length

  //TODO Configurable sample parameters

  private val sampleYear0 = 2018
  private val sampleYear: Year = Year.of(sampleYear0)
  private val visitsFromTimestamp: OffsetDateTime = sampleYear.atDay(1)
    .atStartOfDay()
    .atOffset(ZoneOffset.UTC)
  private val visitsIntervalHours: Long = TimeUnit.DAYS.toHours(sampleYear.length().toLong)

  /* Goes somewhere every day over the year */
  private val maxVisitCountsPerPerson: Int = sampleYear.length()

  def generate()(implicit spark: SparkSession): Unit = {
    val locationVisitsPersons = withPersons
    val locationVisitsTimestamps = withTimestamps(locationVisitsPersons)
    val locationVisitsGeo = withGeoLocations(locationVisitsTimestamps)
    val locationVisits = locationVisitsGeo
      .repartition(col("region_id"), col("year_month"))
      .cache()

    printLocationVisits(locationVisits)
    writeLocationVisits(locationVisits)
  }

  private def withPersons(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    (minPersonId until minPersonId + personCount)
      .toDF("person_id")
      .withColumn("factor", rand(0L))
      .as[(Long, Double)]
      .flatMap { case (personId, factor) =>
        Seq.fill((factor * maxVisitCountsPerPerson).toInt + 1)(personId)
      }
      .toDF("person_id")
  }

  private def withTimestamps(input: DataFrame): DataFrame = {
    val generateTimestampUdf = udf { factor: Double =>
      val offsetHours: Long = (visitsIntervalHours * factor).toLong
      val time = visitsFromTimestamp.plus(offsetHours, ChronoUnit.HOURS)
      new Timestamp(time.toInstant.toEpochMilli)
    }

    input
      .withColumn("factor", rand(0L))
      .withColumn("timestamp", generateTimestampUdf(col("factor")))
      .drop("factor")
      .withColumn(
        "year_month",
        concat(
          format_string("%04d", year(col("timestamp"))),
          format_string("%02d", month(col("timestamp")))
        )
      )
  }

  private def withGeoLocations(input: DataFrame): DataFrame = {
    val regionUdf = udf { regionId: Long =>
      regions.find(_.id == regionId)
        .get
    }

    input
      .withColumn("region_id", rand(0L) * regionCount cast IntegerType)
      .withColumn("region", regionUdf(col("region_id")))
      .withColumn("latitude_factor", randn(0))
      .withColumn("longitude_factor", randn(0))
      .withColumn(
        "latitude",
        expr("region.minLatitude + (region.maxLatitude - region.minLatitude) * latitude_factor") cast DoubleType
      )
      .withColumn(
        "longitude",
        expr("region.minLongitude + (region.maxLongitude - region.minLongitude) * longitude_factor") cast DoubleType
      )
      .drop(
        "region",
        "latitude_factor",
        "longitude_factor"
      )
  }

  private def printLocationVisits(locationVisits: DataFrame): Unit = {
    println(s"Location visits sample size: ${locationVisits.count()}")
    println("Location visits min/max timestamp:")
    locationVisits
      .select(min("timestamp"), max("timestamp"))
      .show(false)
    println("Location visit counts by regions:")
    locationVisits
      .groupBy("region_id")
      .count()
      .show(false)
    val visitorsCount = locationVisits
      .select("person_id")
      .distinct()
      .count()
    println(s"Visitors total count: $visitorsCount")
    val topN = 10
    println(s"Top $topN visitors:")
    locationVisits
      .groupBy("person_id")
      .count()
      .orderBy(col("count").desc)
      .limit(topN)
      .show(false)
    println("Location visits sample:")
    locationVisits.show(false)
  }

  private def writeLocationVisits(locationVisits: DataFrame)(implicit config: SampleGeneratorConfig): Unit = {
    locationVisits
      .write
      .partitionBy("region_id", "year_month")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/location_visits_sample")
  }

}
