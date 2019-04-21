package com.github.tashoyan.visitor.recommender

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object PlaceSimilarPlace {

  val placeSimilarityIntervalDays: Long = 7L
  val placeSimilarityIntervalMillis: Long = TimeUnit.DAYS.toMillis(placeSimilarityIntervalDays)

  val similarPlacesTopN: Int = 50

  def calcPlaceSimilarPlaceEdges(placeVisits: DataFrame): DataFrame = {
    val thatPlaceVisits = placeVisits
      .select(
        col("person_id"),
        col("place_id") as "that_place_id",
        col("timestamp") as "that_timestamp"
      )

    val isInPlaceSimilarityIntervalUdf = udf { (ts1: Timestamp, ts2: Timestamp) =>
      math.abs(ts1.getTime - ts2.getTime) <= placeSimilarityIntervalMillis
    }
    val samePersonVisitCounts = placeVisits
      .join(thatPlaceVisits, "person_id")
      .where(
        (col("place_id") =!= col("that_place_id")) and
          isInPlaceSimilarityIntervalUdf(col("timestamp"), col("that_timestamp"))
      )
      .groupBy("place_id", "that_place_id")
      .agg(count("person_id") as "visit_count")

    val window = Window.partitionBy("place_id")
      .orderBy(col("visit_count").desc)
    val topSimilarPlaces = samePersonVisitCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= similarPlacesTopN)
      .drop("rank")
      .cache()
    println("topSimilarPlaces")
    topSimilarPlaces.show(false)
    topSimilarPlaces.printSchema()

    val totalPlaceVisitCounts = topSimilarPlaces
      .groupBy("place_id")
      .agg(sum("visit_count") as "total_visit_count")
      // Workaround for https://issues.apache.org/jira/browse/SPARK-14948
      .withColumnRenamed("place_id", "renamed_place_id")
    println("totalPlaceVisitCounts")
    totalPlaceVisitCounts.show(false)
    totalPlaceVisitCounts.printSchema()

    //TODO How to partition?
    topSimilarPlaces
      .join(totalPlaceVisitCounts, col("place_id") === col("renamed_place_id"))
      .withColumn("weight", col("visit_count") / col("total_visit_count") cast DoubleType)
      .select(
        col("place_id") as "source_id",
        col("that_place_id") as "target_id",
        col("weight")
      )
  }

}
