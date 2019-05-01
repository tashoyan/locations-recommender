package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.knn.KnnRecommenderConfig._

case class KnnRecommenderConfig(
    dataDir: String = "",
    maxRecommendations: Int = defaultMaxRecommendations
)

object KnnRecommenderConfig {
  val defaultMaxRecommendations: Int = 10
}
