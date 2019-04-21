package com.github.tashoyan.visitor.sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class PlacesSampleGenerator(
    regions: Seq[Region],
    placeCategories: Seq[String]
)(implicit val config: SampleGeneratorConfig) extends Serializable {

  val placeCountPerRegion = 100

  def generate()(implicit spark: SparkSession): Unit = {
    val categories = generateCategories

    val placesGeo = generatePlacesGeo
    val placesCategories = generatePlacesCategories(placesGeo, categories)
    val placesNames = generatePlacesNames(placesCategories)
    val places = placesNames
      .repartition(col("region_id"), col("category_id"))
      .cache()

    printPlaces(places)
    writePlaces(places)
    writeCategories(categories)
  }

  private def generateCategories(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    placeCategories
      .zipWithIndex
      .toDF("category", "category_id")
  }

  private def generatePlacesGeo(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val regionsDs: Dataset[Region] = regions.toDS()
    regionsDs
      .flatMap(generatePlaces)
      .withColumnRenamed("regionId", "region_id")
      .withColumn("id", monotonically_increasing_id())
  }

  private def generatePlaces(region: Region): Iterable[Place] = {
    val latCount, lonCount = math.sqrt(placeCountPerRegion.toDouble).floor.toInt
    val latStep = (region.maxLatitude - region.minLatitude) / latCount
    val lonStep = (region.maxLongitude - region.minLongitude) / lonCount
    val grid: Seq[(Int, Int)] = for {
      latIdx <- 1 to latCount
      lonIdx <- 1 to lonCount
    } yield (latIdx, lonIdx)
    grid.map { case (latIdx, lonIdx) =>
      val latitude = region.minLatitude + latStep * latIdx
      val longitude = region.minLongitude + lonStep * lonIdx
      generatePlace(region, latitude, longitude)
    }
  }

  private def generatePlace(region: Region, latitude: Double, longitude: Double): Place = {
    Place(
      latitude = latitude,
      longitude = longitude,
      regionId = region.id
    )
  }

  private def generatePlacesCategories(input: DataFrame, categories: DataFrame): DataFrame = {
    val categoryIdUdf = udf { factor: Double =>
      (factor * placeCategories.length).toLong
    }
    input
      .withColumn("factor", rand(0L))
      .withColumn("category_id", categoryIdUdf(col("factor")))
      .drop("factor")
      .join(broadcast(categories), "category_id")
  }

  private def generatePlacesNames(input: DataFrame): DataFrame = {
    input
      .withColumn(
        "name",
        concat_ws(
          "-",
          col("category"),
          col("id")
        )
      )
      .withColumn("description", col("name"))
  }

  private def printPlaces(places: DataFrame): Unit = {
    println(s"Places sample size: ${places.count()}")
    println("Place counts by regions:")
    places
      .groupBy("region_id")
      .count()
      .show(false)
    println("Place counts by categories:")
    places
      .groupBy("category")
      .count()
      .show(false)
    println("Places sample:")
    places.show(false)
  }

  private def writePlaces(places: DataFrame)(implicit config: SampleGeneratorConfig): Unit = {
    places
      .write
      .partitionBy("region_id", "category")
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/places_sample")
  }

  private def writeCategories(categories: DataFrame)(implicit config: SampleGeneratorConfig): Unit = {
    categories
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/categories_sample")
  }

}
