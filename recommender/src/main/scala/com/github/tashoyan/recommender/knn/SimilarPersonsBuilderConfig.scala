package com.github.tashoyan.recommender.knn

case class SimilarPersonsBuilderConfig(
    dataDir: String = "",
    placeWeight: Double = 0.0,
    categoryWeight: Double = 0.0,
    kNearest: Int = 0
)
