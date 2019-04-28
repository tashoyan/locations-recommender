package com.github.tashoyan.visitor.recommender.knn

case class SimilarPersonsBuilderConfig(
    samplesDir: String = "",
    alphaPlace: Double = 0.0,
    alphaCategory: Double = 0.0,
    kNearest: Int = 0
)
