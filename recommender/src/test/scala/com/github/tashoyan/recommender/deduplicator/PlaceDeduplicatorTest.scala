package com.github.tashoyan.recommender.deduplicator

import com.github.tashoyan.recommender.test.SparkTestHarness
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.scalatest.Inside._
import org.scalatest.Matchers._

class PlaceDeduplicatorTest extends FunSuite with SparkTestHarness {

  private val placeColumns = Array("id", "name", "latitude", "longitude")

  private val confirmedPlaces = Seq(
    (1L, "Ryumochnaya v Zuzino", 55.656892, 37.596852)
  )

  private def confirmedPlacesDf: DataFrame = {
    val spark0 = spark
    import spark0.implicits._
    confirmedPlaces
      .toDF(placeColumns: _*)
  }

  test("dropDuplicates") {
    val spark0 = spark
    import spark0.implicits._
    val placesDf = Seq(
      /* Misspelled name */
      (101L, "rumochnaya zuzino", 55.656892, 37.596852),
      /* Imprecise coordinates*/
      (102L, "Ryumochnaya v Zuzino", 55.656686, 37.597599),
      /* A place completely different from all confirmed places*/
      (103L, "Biryulyovo Tovarnaya", 55.593259, 37.653726)
    )
      .toDF(placeColumns: _*)

    val deduplicator = new PlaceDeduplicator(maxPlaceDistanceMeters = 60, maxNameDifference = 5)
    val noDuplicatePlaces = deduplicator.dropDuplicates(placesDf, confirmedPlacesDf)
      .as[(Long, String, Double, Double)]
      .collect()

    noDuplicatePlaces.length should be(1)
    inside(noDuplicatePlaces.head) { case (id, name, _, _) =>
      id should be(103L)
      name should be("Biryulyovo Tovarnaya")
    }
  }

}
