package com.github.tashoyan.recommender.stochastic

import StochasticRecommenderConfig._

case class StochasticRecommenderConfig(
    dataDir: String = "",
    epsilon: Double = defaultEpsilon,
    maxIterations: Int = defaultMaxIterations,
    maxRecommendations: Int = defaultMaxRecommendations
)

object StochasticRecommenderConfig {
  val defaultEpsilon: Double = 0.05
  val defaultMaxIterations: Int = 20
  val defaultMaxRecommendations: Int = 10
}
