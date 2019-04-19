package com.github.tashoyan.visitor.sample

case class Region(
    id: Long,
    name: String,
    minLatitude: Double,
    maxLatitude: Double,
    minLongitude: Double,
    maxLongitude: Double
) {
  require(minLatitude < maxLatitude, s"Min latitude $minLatitude must be less than max latitude $maxLatitude")
  require(minLongitude < maxLongitude, s"Min longitude $minLongitude must be less than max longitude $maxLongitude")
}
