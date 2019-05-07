package com.github.tashoyan.recommender.knn

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object RatingVectorsBuilder {

  def calcRatingVectors(
      ratings: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      vectorColumn: String
  )(implicit spark: SparkSession): DataFrame = {
    val vectorSize: Int = calcRatingVectorSize(ratings.cache(), entityIdColumn)
    aggRatingVectors(
      ratings,
      entityIdColumn,
      ratingColumn,
      vectorColumn,
      vectorSize
    )
  }

  private def calcRatingVectorSize(ratings: DataFrame, entityIdColumn: String)(implicit spark: SparkSession): Int = {
    import spark.implicits._

    val maxId = ratings
      .select(max(entityIdColumn))
      .as[Long]
      .head()
    checkedCast(maxId) + 1
  }

  private def checkedCast(l: Long): Int = {
    if (l.isValidInt)
      l.toInt
    else
      throw new ArithmeticException(s"Index out of Int range: $l")
  }

  case class Elem(index: Int, value: Double)

  object Elem {
    implicit val elemOrdering: Ordering[Elem] = Ordering.by(_.index)
  }

  //TODO Better name
  type ElemAgg = scala.collection.mutable.TreeSet[Elem]

  private def aggRatingVectors(
      ratings: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      vectorColumn: String,
      vectorSize: Int
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ratingsRdd: RDD[(Long, Elem)] = ratings
      .select(
        "person_id",
        entityIdColumn,
        ratingColumn
      )
      .as[(Long, Long, Long)]
      .rdd
      .map { case (personId, entityId, rating) => (personId, Elem(checkedCast(entityId), rating.toDouble)) }

    val aggregatedRatingsRdd = ratingsRdd
      .aggregateByKey(new ElemAgg(), ratingsRdd.getNumPartitions)(_ += _, _ ++= _)

    def toSparseVector(agg: ElemAgg): SparseVector = {
      val (indexes, values) = agg.map(e => (e.index, e.value)).toArray.unzip
      new SparseVector(vectorSize, indexes, values)
    }

    val ratingVectorsRdd: RDD[(Long, SparseVector)] = aggregatedRatingsRdd.mapValues(toSparseVector)

    val ratingVectors = spark.createDataset[(Long, SparseVector)](ratingVectorsRdd)
      .toDF("person_id", vectorColumn)
    ratingVectors
  }

}
