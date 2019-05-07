package com.github.tashoyan.recommender.knn

case class RatingVectorsBuilderConfig(
    dataDir: String = "",
    lastDaysCount: Int = 0,
    maxRatedPlaces: Int = 0,
    maxRatedCategories: Int = 0
)
