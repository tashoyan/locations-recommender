package com.github.tashoyan.visitor.recommender

import com.github.tashoyan.visitor.recommender.Location._
import org.apache.commons.math3.util.FastMath._

case class Location(latitude: Double, longitude: Double) {
  require(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE, s"Latitude $latitude must be within range [$MIN_LATITUDE, $MAX_LATITUDE]")
  require(longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE, s"Longitude $longitude must be within range [$MIN_LONGITUDE, $MAX_LONGITUDE]")

  /**
    * Calculates the distance to another location.
    * Uses [[https://en.wikipedia.org/wiki/Haversine_formula Haversine formula]].
    *
    * @param that The other location to calculate distance.
    * @return Distance in meters.
    */
  def distanceMeters(that: Location): Double = {
    Location.distanceMeters(this, that)
  }
}

object Location {
  val MIN_LATITUDE: Double = -90.0
  val MAX_LATITUDE: Double = 90.0
  val MIN_LONGITUDE: Double = -180.0
  val MAX_LONGITUDE: Double = 180.0

  val earthRadiusMeters: Double = 6371 * 1000

  def distanceMeters(location1: Location, location2: Location): Double = {
    val lat1 = toRadians(location1.latitude)
    val lat2 = toRadians(location2.latitude)
    val lon1 = toRadians(location1.longitude)
    val lon2 = toRadians(location2.longitude)
    val hav = haversine(lat2 - lat1) +
      cos(lat1) * cos(lat2) * haversine(lon2 - lon1)
    earthRadiusMeters * 2 * asin(sqrt(hav))
  }

  def haversine(theta: Double): Double = {
    val s = sin(theta / 2)
    s * s
  }

}
