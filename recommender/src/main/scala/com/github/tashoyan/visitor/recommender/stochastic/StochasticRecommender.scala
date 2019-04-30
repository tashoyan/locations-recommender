package com.github.tashoyan.visitor.recommender.stochastic

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
  require(epsilon >= 0, "epsilon must be non-negative")
  require(maxIterations >= 0, "max iterations number must be non-negative")

  import spark.implicits._

  private val alpha: Double = 0.15

  private val epsilonSquared = epsilon * epsilon

  private lazy val vertexes: DataFrame = {
    val sourceVertexes = stochasticEdges.select(col("source_id") as "id")
    val targetVertexes = stochasticEdges.select(col("target_id") as "id")
    (sourceVertexes union targetVertexes)
      .distinct()
      .repartition(col("id"))
      .cache()
  }

  private lazy val vertexCount: Long = vertexes.count()

  private lazy val x0: DataFrame = vertexes
    .withColumn("probability", lit(1.0) / vertexCount.toDouble)

  /**
    * Make recommendations for the given vertex.
    *
    * @param vertexId Id of the vertex.
    *                 The algorithm will find a list of vertexes,
    *                 that are reachable with highest probability from this vertex.
    * @return Data frame of the most recommended vertexes for the given vertex: `(vertex_id, probability)`
    *         Ordered by probability descending.
    * @throws IllegalArgumentException No such vertex in the graph.
    */
  def makeRecommendations(vertexId: Long): DataFrame = {
    if (isVertexExist(vertexId))
      makeRecommendations0(vertexId)
    else
      throw new IllegalArgumentException(s"No such vertex in the graph: $vertexId")
  }

  private def isVertexExist(vertexId: Long): Boolean = {
    //TODO More optimal way to check if non-empty
    vertexes
      .where(col("id") === vertexId)
      .count() > 0
  }

  private def makeRecommendations0(vertexId: Long): DataFrame = {
    val u = vertexes
      .withColumn("u_probability", when(col("id") === vertexId, 1.0) otherwise 0.0)
      .cache()
    val stationaryX = step(x0, u, 0)
    val recommendedVertexes = stationaryX
      .where(
        col("id") =!= vertexId and
          col("probability") > 0
      )
    recommendedVertexes
  }

  @tailrec private def step(x: DataFrame, u: DataFrame, iteration: Int): DataFrame = {
    if (iteration >= maxIterations) {
      println(s"Number of iterations $iteration reached the maximum $maxIterations")
      x
    } else {
      val nextX = calcNextX(x, u)
        .cache()
      if (isConverged(x, nextX)) {
        println(s"Converged in $iteration iterations")
        nextX
      } else {
        step(nextX, u, iteration + 1)
      }
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
    val diffSquaredDf = nextX
      .withColumnRenamed("probability", "next_probability")
      .join(x, "id")
      .select(col("next_probability") - col("probability") as "diff")
      .select(col("diff") * col("diff") as "diff_squared")
      .as[Double]
    val diffSquared = diffSquaredDf
      .rdd
      .sum()
    diffSquared <= epsilonSquared
  }

}
