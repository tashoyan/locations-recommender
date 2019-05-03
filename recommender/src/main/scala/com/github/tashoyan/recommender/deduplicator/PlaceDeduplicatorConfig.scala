package com.github.tashoyan.recommender.deduplicator

import PlaceDeduplicatorConfig._

case class PlaceDeduplicatorConfig(
    dataDir: String = "",
    maxPlaceDistanceMeters: Double = defaultMaxPlaceDistanceMeters,
    maxNameDifference: Int = defaultMaxNameDifference
)

object PlaceDeduplicatorConfig {
  val defaultMaxPlaceDistanceMeters: Double = 100
  val defaultMaxNameDifference: Int = 5
}
