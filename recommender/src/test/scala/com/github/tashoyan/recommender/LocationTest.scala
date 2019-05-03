package com.github.tashoyan.recommender

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class LocationTest extends FunSuite {

  test("distance - same location") {
    val location1 = Location(55.6438965, 37.4433515)
    val result = location1 distanceMeters location1
    result should be(0)
  }

  test("distance - two distinct locations") {
    val location1 = Location(55.612652, 37.591753)
    val location2 = Location(55.611152, 37.603366)
    val result = location1 distanceMeters location2
    result shouldBe 745.0 +- 5.0
  }

  test("distance - commutative") {
    val location1 = Location(55.612652, 37.591753)
    val location2 = Location(55.611152, 37.603366)
    val result1 = location1 distanceMeters location2
    val result2 = location2 distanceMeters location1
    result1 should equal(result2)
  }

}
