package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

/**
  * Recommender based on random walk over a graph.
  * The graph must have [[https://en.wikipedia.org/wiki/Stochastic_matrix stochastic edges]]:
  * for each vertex, the sum of probabilities of all outbound edges is 1.0.
  * <p>
  * The recommender walks over the graph and iteratively improves the recommendation
  * until one of the two conditions is met:
  * <ul>
  * <li> iterations are converged with the accuracy `epsilon`
  * <li> maximum number of iterations is reached
  * </ul>
  *
  * @param stochasticEdges Data frame representing the directed stochastic graph: `(source_id, target_id, probability)`.
  *                        For each `source_id`, the sum of probabilities to all targets is 1.0.
  *                        A missing pair `(source_id, target_id)` should be interpreted as zero probability for this pair.
  * @param epsilon         Expected accuracy of recommendations.
  *                        If on the iteration `t` the difference `||x(t) - x(t-1)|| &lt; epsilon`, then the algorithm is converged.
  * @param maxIterations   Maximum number of iterations.
  *                        The algorithm stops after the maximum iterations number is reached.
  */
class StochasticRecommender(
                             stochasticEdges: DataFrame,
                             epsilon: Double,
                             maxIterations: Int
                           )(implicit spark: SparkSession) {

  import spark.implicits._

  private val alpha: Double = 0.15

  private val epsilonSquared = epsilon * epsilon

  lazy val vertexes: DataFrame = {
    val sourceVertexes = stochasticEdges.select(col("source_id") as "id")
    val targetVertexes = stochasticEdges.select(col("target_id") as "id")
    (sourceVertexes union targetVertexes)
      .distinct()
      .cache()
  }

  lazy val vertexCount: Long = vertexes.count()

  lazy val x0: DataFrame = vertexes
    .withColumn("probability", lit(1.0) / vertexCount.toDouble)

  /**
    * Make recommendations for the given vertex.
    *
    * @param vertexId           Id of the vertex.
    *                           The algorithm will find a list of vertexes,
    *                           that are reachable with highest probability from this vertex.
    * @param maxRecommendations How many recommended vertexes to return.
    * @return List of the most recommended vertexes for the given vertex.
    *         Most probable vertexes first.
    */
  def makeRecommendation(vertexId: Long, maxRecommendations: Int): Seq[(Long, Double)] = {
    val u = vertexes
      .withColumn("u_probability", when(col("id") === vertexId, 1.0) otherwise 0.0)
      .cache()
    val stationaryX = step(x0, u, 1)
    val recommendedVertexes = stationaryX
      .where(col("id") =!= vertexId)
      .orderBy(col("probability").desc)
      .limit(maxRecommendations)
      .as[(Long, Double)]
    recommendedVertexes.collect()
  }

  @tailrec private def step(x: DataFrame, u: DataFrame, iteration: Int): DataFrame = {
    if (iteration >= maxIterations) {
      x
    } else {
      val nextX = calcNextX(x, u)
        .cache()
      if (isConverged(x, nextX))
        nextX
      else
        step(nextX, u, iteration + 1)
    }
  }

  private def calcNextX(x: DataFrame, u: DataFrame): DataFrame = {
    val sigma = x
      .join(stochasticEdges, col("id") === col("source_id"))
      .na.fill(0.0, Seq("probability"))
      .withColumn("acc", col("probability") * col("balanced_weight"))
      .groupBy("target_id")
      .agg(sum("acc") as "sigma")
    val nextX = u
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
    nextX
  }

  private def isConverged(x: DataFrame, nextX: DataFrame): Boolean = {
    val diffSquared = nextX
      .withColumnRenamed("probability", "next_probability")
      .join(x, "id")
      .select(
        pow(col("next_probability") - col("probability"), 2) as "diff_squared"
      )
      .as[Double]
      .rdd
      .sum()
    diffSquared <= epsilonSquared
  }

}
