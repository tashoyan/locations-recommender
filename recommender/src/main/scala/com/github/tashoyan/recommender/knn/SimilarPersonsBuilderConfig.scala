package com.github.tashoyan.recommender.knn

case class SimilarPersonsBuilderConfig(
    dataDir: String = "",
    lastDaysCount: Int = 0,
    placeWeight: Double = 0.0,
    categoryWeight: Double = 0.0,
    kNearest: Int = 0
)
