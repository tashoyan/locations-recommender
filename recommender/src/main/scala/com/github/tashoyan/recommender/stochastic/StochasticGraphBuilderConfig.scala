package com.github.tashoyan.recommender.stochastic

case class StochasticGraphBuilderConfig(
    dataDir: String = "",
    lastDaysCount: Int = 0,
    betaPersonPlace: Double = 0.0,
    betaPersonCategory: Double = 0.0
)
