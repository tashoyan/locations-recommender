package com.github.tashoyan.visitor.recommender.deduplicator

import com.github.tashoyan.visitor.recommender.DataUtils
import org.apache.spark.sql.SparkSession

object PlaceDeduplicatorMain extends PlaceDeduplicatorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, PlaceDeduplicatorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: PlaceDeduplicatorConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val places = DataUtils.loadPlaces(config.dataDir)
    places.show(false)
  }

}
