package com.github.tashoyan.visitor.sample

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneOffset}

import com.github.tashoyan.visitor.test.SparkTestHarness
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SampleGeneratorTest extends FunSuite with SparkTestHarness {

  test("generate visits raw sample") {
    val input = this.getClass
      .getResource("sample264")
      .toString
    val inputData = spark.read.parquet(input)
    println(s"Input data count: ${inputData.count()}")

    val from = LocalDate.of(2018, 1, 1)
      .atStartOfDay()
      .atOffset(ZoneOffset.UTC)
    from.plus(10, ChronoUnit.HOURS)
    val intervalHours = 365 * 24
    val generateTimestampUdf = udf { factor: Double =>
      val offsetHours: Long = (intervalHours * factor).toLong
      val time = from.plus(offsetHours, ChronoUnit.HOURS)
      new Timestamp(time.toInstant.toEpochMilli)
    }
    val sample = inputData
      .withColumn("random", rand(0L))
      .withColumn("timestamp", generateTimestampUdf(col("random")))
      .select(
        col("userId") as "personId",
        col("artistId") as "locationId",
        col("timestamp")
      )
    println("Visits raw sample:")
    sample.show(false)
    println("Visits min/max timestamp:")
    sample.select(min("timestamp"), max("timestamp")).show(false)
    sample
      .repartition(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(s"${sys.props("java.io.tmpdir")}/visits_raw_sample")
  }

}
