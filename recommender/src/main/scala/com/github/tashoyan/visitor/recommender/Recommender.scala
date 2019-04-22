package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Recommender {

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
      .withColumn("probability", when(col("id") === userId, 1.0) otherwise 0.0)

    x0.orderBy("id").show(false)
    u.orderBy("id").show(false)


  }

}
