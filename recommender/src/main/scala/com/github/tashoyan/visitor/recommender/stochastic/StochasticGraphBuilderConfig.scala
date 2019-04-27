package com.github.tashoyan.visitor.recommender.stochastic

case class StochasticGraphBuilderConfig(
    samplesDir: String = "",
    betaPersonPlace: Double = 0.0,
    betaPersonCategory: Double = 0.0
)
