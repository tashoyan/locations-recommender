package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Recommender {
  val alpha: Double = 0.15

  def recommend(
      visitGraph: DataFrame,
      userId: Long
  ): Unit = {
    val sourceVertexes = visitGraph.select(col("source_id") as "id")
    val targetVertexes = visitGraph.select(col("target_id") as "id")
    val vertexes = (sourceVertexes union targetVertexes)
      .distinct()
      .cache()
    val vertexCount = vertexes.count()
    val x0 = vertexes
      .withColumn("probability", lit(1.0) / vertexCount cast DoubleType)
    val u = vertexes
      .withColumn("u_probability", when(col("id") === userId, 1.0) otherwise 0.0)

    //    println("x0:")
    //    x0.orderBy("id").show(false)
    //    println("u:")
    //    u.orderBy("id").show(false)

    val sigma = x0
      .join(visitGraph, col("id") === col("source_id"))
      .na.fill(0.0, Seq("probability"))
      .withColumn("acc", col("probability") * col("balanced_weight"))
      .groupBy("target_id")
      .agg(sum("acc") as "sigma")
    //    println("sigma:")
    //    sigma.orderBy("target_id").show(false)

    val x = u
      .join(sigma, col("id") === col("target_id"), "left")
      .na.fill(0.0, Seq("sigma"))
      .withColumn(
        "next_probability",
        col("u_probability") * alpha +
          col("sigma") * (1 - alpha)
      )
      .select(
        col("id"),
        col("next_probability") as "probability"
      )
    println("x:")
    x.orderBy("id").show(false)
  }

}
