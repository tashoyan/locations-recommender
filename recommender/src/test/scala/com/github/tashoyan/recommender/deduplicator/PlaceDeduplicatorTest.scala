package com.github.tashoyan.recommender.deduplicator

import com.github.tashoyan.recommender.test.SparkTestHarness
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Inside._
import org.scalatest.Matchers._
import org.scalatest.fixture

class PlaceDeduplicatorTest extends fixture.FunSuite with SparkTestHarness {

  private val placeColumns = Array("region_id", "id", "name", "latitude", "longitude")

  private val confirmedPlaces = Seq(
    (0L, 1L, "Ryumochnaya v Zuzino", 55.656892, 37.596852)
  )

  private def confirmedPlacesDf(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    confirmedPlaces
      .toDF(placeColumns: _*)
  }

  test("dropDuplicates") { implicit spark: SparkSession =>
    import spark.implicits._
    val placesDf = Seq(
      /* Misspelled name */
      (0L, 101L, "rumochnaya zuzino", 55.656892, 37.596852),
      /* Imprecise coordinates*/
      (0L, 102L, "Ryumochnaya v Zuzino", 55.656686, 37.597599),
      /* A place completely different from all confirmed places*/
      (0L, 103L, "Biryulyovo Tovarnaya", 55.593259, 37.653726)
    )
      .toDF(placeColumns: _*)

    val deduplicator = new PlaceDeduplicator(maxPlaceDistanceMeters = 60, maxNameDifference = 5)
    val noDuplicatePlaces = deduplicator.dropDuplicates(placesDf, confirmedPlacesDf)
      .as[(Long, Long, String, Double, Double)]
      .collect()

    noDuplicatePlaces.length should be(1)
    inside(noDuplicatePlaces.head) { case (_, id, name, _, _) =>
      id should be(103L)
      name should be("Biryulyovo Tovarnaya")
    }
  }

}
