package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait RecommenderMainCommon {

  protected case class RecommenderTarget(
      personId: Long,
      homeRegionId: Long,
      targetRegionId: Long
  )

  private val inputRegex = """(\d+)\s*(\d+)?""".r

  protected def parseInput(input: String): (Long, Option[Long]) = {
    input match {
      case inputRegex(personIdStr, regionIdStr) =>
        (personIdStr.toLong, Option(regionIdStr).map(_.toLong))
      case _ =>
        throw new IllegalArgumentException(s"Failed to parse input: $input")
    }
  }

  protected def calcRecommenderTarget(persons: DataFrame)(personIdInputRegionId: (Long, Option[Long]))(implicit spark: SparkSession): Try[RecommenderTarget] = {
    val personId = personIdInputRegionId._1
    val inputRegionId = personIdInputRegionId._2
    val tryHomeRegionId = getHomeRegionId(personId, persons)
    tryHomeRegionId map calcRecommenderTarget(personId, inputRegionId)
  }

  private def calcRecommenderTarget(personId: Long, inputRegionId: Option[Long])(homeRegionId: Long): RecommenderTarget = {
    val targetRegionId = inputRegionId.getOrElse {
      Console.out.println("Target region ID is not provided - falling back to the person's home region")
      homeRegionId
    }
    RecommenderTarget(personId, homeRegionId, targetRegionId)
  }

  private def getHomeRegionId(personId: Long, persons: DataFrame)(implicit spark: SparkSession): Try[Long] = {
    import spark.implicits._

    val regionIds = persons
      .where(col("id") === personId)
      .limit(1)
      .select("home_region_id")
      .as[Long]
      .collect()
    Try(
      regionIds
        .headOption
        .getOrElse(throw new NoSuchElementException(s"Person not found: $personId"))
    )
  }

}
