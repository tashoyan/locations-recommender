package com.github.tashoyan.recommender.knn

import KnnRecommenderConfig._

case class KnnRecommenderConfig(
    dataDir: String = "",
    maxRecommendations: Int = defaultMaxRecommendations
)

object KnnRecommenderConfig {
  val defaultMaxRecommendations: Int = 10
}
