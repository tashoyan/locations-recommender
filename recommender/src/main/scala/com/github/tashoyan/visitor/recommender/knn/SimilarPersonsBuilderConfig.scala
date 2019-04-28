package com.github.tashoyan.visitor.recommender.knn

case class SimilarPersonsBuilderConfig(
    samplesDir: String = "",
    placeWeight: Double = 0.0,
    categoryWeight: Double = 0.0,
    kNearest: Int = 0
)
