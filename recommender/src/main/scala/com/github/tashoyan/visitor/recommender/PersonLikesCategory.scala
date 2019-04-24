package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum}
import org.apache.spark.sql.types.DoubleType

object PersonLikesCategory {

  val visitedCategoriesTopN: Int = 100

  def calcPersonLikesCategoryEdges(placeVisits: DataFrame): DataFrame = {
    val personVisitCategoryCounts = placeVisits
      .groupBy("person_id", "category_id")
      .agg(count("*") as "visit_count")

    val window = Window.partitionBy("person_id")
      .orderBy(col("visit_count").desc)
    val topVisitedCategories = personVisitCategoryCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= visitedCategoriesTopN)
      .drop("rank")
      .cache()

    val totalVisitedCategories = topVisitedCategories
      .groupBy("person_id")
      .agg(sum("visit_count") as "total_visit_count")

    topVisitedCategories
      .join(totalVisitedCategories, "person_id")
      .withColumn("weight", col("visit_count") / col("total_visit_count") cast DoubleType)
      .select(
        col("person_id") as "source_id",
        col("category_id") as "target_id",
        col("weight")
      )
  }

}
