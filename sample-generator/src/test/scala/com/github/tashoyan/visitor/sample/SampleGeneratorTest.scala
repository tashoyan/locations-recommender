package com.github.tashoyan.visitor.sample

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, OffsetDateTime, ZoneOffset}

import com.github.tashoyan.visitor.sample.SampleGeneratorTest._
import com.github.tashoyan.visitor.test.SparkTestHarness
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.scalatest.FunSuite

//TODO Move from tests to main, add launcher
class SampleGeneratorTest extends FunSuite with SparkTestHarness {

  ignore("generate location visits sample") {
    val spark0 = spark
    import spark0.implicits._

    val locationVisitsPersons = (0 until personCount)
      .toDF("person_id")
      .withColumn("factor", rand(0L))
      .as[(Long, Double)]
      .flatMap { case (personId, factor) =>
        Seq.fill((factor * maxVisitCountyesPerPerson).toInt + 1)(personId)
      }
      .toDF("person_id")

    val generateTimestampUdf = udf { factor: Double =>
      val offsetHours: Long = (visitsIntervalHours * factor).toLong
      val time = visitsFromTimestamp.plus(offsetHours, ChronoUnit.HOURS)
      new Timestamp(time.toInstant.toEpochMilli)
    }
    val locationVisitsTimestamps = locationVisitsPersons
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

    val regionUdf = udf { regionId: Long =>
      regions.find(_.id == regionId)
        .get
    }
    val locationVisitsGeo = locationVisitsTimestamps
      .withColumn("region_id", rand(0L) * regionCount cast IntegerType)
      .withColumn("region", regionUdf(col("region_id")))
      .withColumn("random_latitude", randn(0))
      .withColumn("random_longitude", randn(0))
      .withColumn(
        "latitude",
        expr("region.minLatitude + (region.maxLatitude - region.minLatitude) * random_latitude") cast DoubleType
      )
      .withColumn(
        "longitude",
        expr("region.minLongitude + (region.maxLongitude - region.minLongitude) * random_longitude") cast DoubleType
      )
      .drop("region", "random_latitude", "random_longitude")

    val locationVisits = locationVisitsGeo.cache()

    //TODO function: printLocationVisitsSample
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
    println("Top 10 visitors:")
    locationVisits
      .groupBy("person_id")
      .count()
      .orderBy(col("count").desc)
      .limit(10)
      .show(false)
    println("Location visits sample:")
    locationVisits.show(false)

    locationVisits
      .write
      .partitionBy("year_month", "region_id")
      .mode(SaveMode.Overwrite)
      .parquet(s"${sys.props("java.io.tmpdir")}/location_visits_sample")
  }

}

object SampleGeneratorTest {

  val personCount: Int = 10000
  val maxVisitCountyesPerPerson: Int = 10

  val visitsFromTimestamp: OffsetDateTime = LocalDate.of(2018, 1, 1)
    .atStartOfDay()
    .atOffset(ZoneOffset.UTC)
  val visitsIntervalHours: Int = 365 * 24

  val regions: Seq[Region] = Seq(
    Region(id = 0L, name = "Moscow", minLatitude = 55.623920, maxLatitude = 55.823685, minLongitude = 37.404277, maxLongitude = 37.795022),
    Region(id = 1L, name = "Peterburg", minLatitude = 59.857032, maxLatitude = 60.006462, minLongitude = 30.196832, maxLongitude = 30.490272),
    Region(id = 2L, name = "Kazan", minLatitude = 55.744243, maxLatitude = 55.835127, minLongitude = 49.024581, maxLongitude = 49.231314)
  )
  val regionCount: Int = regions.length
}
