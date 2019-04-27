package com.github.tashoyan.visitor.recommender.stochastic

case class RecommenderConfig(
    samplesDir: String = "",
    epsilon: Double = defaultEpsilon,
    maxIterations: Int = defaultMaxIterations,
    maxRecommendations: Int = defaultMaxRecommendations
)

object RecommenderConfig {
  val defaultEpsilon: Double = 0.05
  val defaultMaxIterations: Int = 20
  val defaultMaxRecommendations: Int = 10
}
