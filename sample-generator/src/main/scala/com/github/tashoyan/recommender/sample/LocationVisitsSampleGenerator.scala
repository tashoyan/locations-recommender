package com.github.tashoyan.recommender.sample

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{OffsetDateTime, Year, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class LocationVisitsSampleGenerator(
    regions: Seq[Region],
    personCount: Long,
    minPersonId: Long
)(implicit val config: SampleGeneratorConfig) extends Serializable {
  private val regionCount: Int = regions.length
  private val personCountPerRegion: Long = personCount / regionCount

  //TODO Configurable sample parameters

  private val sampleYear0 = 2018
  private val sampleYear: Year = Year.of(sampleYear0)
  private val visitsFromTimestamp: OffsetDateTime = sampleYear.atDay(1)
    .atStartOfDay()
    .atOffset(ZoneOffset.UTC)
  private val visitsIntervalHours: Long = TimeUnit.DAYS.toHours(sampleYear.length().toLong)

  /* Goes somewhere every day over the year */
  private val maxVisitCountsPerPerson: Int = sampleYear.length()

  private val regionUdf: UserDefinedFunction = udf { regionId: Long =>
    regions.find(_.id == regionId)
      .get
  }

  def generate()(implicit spark: SparkSession): Unit = {
    val persons = generatePersons
    writePersons(persons)

    val personVisits = withVisits(persons)
    val geoVisits = withGeoLocations(personVisits)
    val timestampLocationVisits = withTimestamps(geoVisits)

    val locationVisits = timestampLocationVisits
      .repartition(col("region_id"), col("year_month"))
      .cache()

    printLocationVisits(locationVisits)
    assertLocationVisitsCorrect(locationVisits)
    writeLocationVisits(locationVisits)
  }

  private def generatePersons(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val regionsDs: Dataset[Long] = regions.map(_.id)
      .toDS()
    regionsDs.flatMap { regionId =>
      val beginPersonId = minPersonId + regionId * personCountPerRegion
      val endPersonId = minPersonId + (regionId + 1) * personCountPerRegion
      (beginPersonId until endPersonId)
        .map(personId => (personId, regionId))
    }
      .toDF("id", "home_region_id")
  }

  private def writePersons(persons: DataFrame)(implicit config: SampleGeneratorConfig): Unit = {
    persons
      .write
      .partitionBy("home_region_id")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.dataDir}/persons_sample")
  }

  private def withVisits(persons: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    persons
      .select(
        col("id"),
        col("home_region_id"),
        rand(0L) as "factor"
      )
      .as[(Long, Long, Double)]
      .flatMap { case (personId, regionId, factor) =>
        Seq.fill((factor * maxVisitCountsPerPerson).toInt + 1)((personId, regionId))
      }
      .toDF("person_id", "region_id")

  }

  private def withGeoLocations(input: DataFrame): DataFrame = {
    input
      .withColumn("region", regionUdf(col("region_id")))
      .withColumn("latitude_factor", rand(0))
      .withColumn("longitude_factor", rand(0))
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

    println("Per-region locations min/max/average:")
    locationVisits
      .groupBy("region_id")
      .agg(
        min(col("latitude")),
        max(col("latitude")),
        avg(col("latitude"))
      ).show(false)
    locationVisits
      .groupBy("region_id")
      .agg(
        min(col("longitude")),
        max(col("longitude")),
        avg(col("longitude"))
      ).show(false)

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

  private def assertLocationVisitsCorrect(locationVisits: DataFrame): Unit = {
    val extLocationVisits = locationVisits
      .withColumn("region", regionUdf(col("region_id")))

    val wrongLocationLatitudes = extLocationVisits
      .where(expr("latitude < region.minLatitude or latitude > region.maxLatitude"))
    val wrongLocationLatitudesCount = wrongLocationLatitudes.count()
    if (wrongLocationLatitudesCount > 0) {
      println(s"$wrongLocationLatitudesCount locations have latitudes out of range for their regions")
      wrongLocationLatitudes.show(false)
      throw new AssertionError("Wrong location visits sample")
    }

    val wrongLocationLongitudes = extLocationVisits
      .where(expr("longitude < region.minLongitude or longitude > region.maxLongitude"))
    val wrongLocationLongitudesCount = wrongLocationLongitudes.count()
    if (wrongLocationLongitudesCount > 0) {
      println(s"$wrongLocationLongitudesCount locations have longitudes out of range for their regions")
      wrongLocationLongitudes.show(false)
      throw new AssertionError("Wrong location visits sample")
    }
  }

  private def writeLocationVisits(locationVisits: DataFrame)(implicit config: SampleGeneratorConfig): Unit = {
    locationVisits
      .write
      .partitionBy("region_id", "year_month")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.dataDir}/location_visits_sample")
  }

}
