package com.github.tashoyan.visitor.sample

import org.apache.spark.sql.SparkSession

object SampleGeneratorMain extends SampleGeneratorArgParser {

  private val regions: Seq[Region] = Seq(
    Region(id = 0L, name = "Moscow", minLatitude = 55.623920, maxLatitude = 55.823685, minLongitude = 37.404277, maxLongitude = 37.795022),
    Region(id = 1L, name = "Peterburg", minLatitude = 59.857032, maxLatitude = 60.006462, minLongitude = 30.196832, maxLongitude = 30.490272),
    Region(id = 2L, name = "Kazan", minLatitude = 55.744243, maxLatitude = 55.835127, minLongitude = 49.024581, maxLongitude = 49.231314)
  )

  private val placeCategories: Seq[String] = Seq(
    "theatre",
    "cinema",
    "museum",
    "shop",
    "gym",
    "stadium",
    "park",
    "cafe",
    "restaurant",
    "coworking",
    "office",
    "gas_station",
    "university",
    "school",
    "kindergarten",
    "police_station",
    "military_base",
    "cemetery",
    "nuclear_power_plant",
    "fallout_shelter"
  )

  private val placeCount: Long = 300L
  private val personCount: Long = 3000L

  private val minCategiryId: Long = 0L
  private val minPlaceId: Long = minCategiryId + placeCategories.length * 2
  private val minPersonId: Long = minPlaceId + placeCount * 2

  def main(args: Array[String]): Unit = {
    parser.parse(args, SampleGeneratorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: SampleGeneratorConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    new LocationVisitsSampleGenerator(
      regions = regions,
      personCount = personCount,
      minPersonId = minPersonId
    )
      .generate()
    new PlacesSampleGenerator(
      regions = regions,
      placeCategories = placeCategories,
      minCategoryId = minCategiryId,
      placeCount = placeCount,
      minPlaceId = minPlaceId
    )
      .generate()

    spark.stop()
  }

}
