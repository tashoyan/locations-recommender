package com.github.tashoyan.recommender.knn

import KnnRecommenderConfig._

case class KnnRecommenderConfig(
    dataDir: String = "",
    placeWeight: Double = 0.0,
    categoryWeight: Double = 0.0,
    kNearest: Int = 0,
    maxRecommendations: Int = defaultMaxRecommendations
)

object KnnRecommenderConfig {
  val defaultMaxRecommendations: Int = 10
}
