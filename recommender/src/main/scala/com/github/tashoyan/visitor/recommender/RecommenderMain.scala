package com.github.tashoyan.visitor.recommender

import org.apache.spark.sql.SparkSession

object RecommenderMain extends RecommenderArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, RecommenderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: RecommenderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    println(spark.conf)
    println(config)
  }

}
