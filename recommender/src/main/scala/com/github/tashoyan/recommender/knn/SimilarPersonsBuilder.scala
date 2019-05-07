package com.github.tashoyan.recommender.knn

import com.github.tashoyan.recommender.knn.Distance._
import com.github.tashoyan.recommender.knn.SimilarPersonsBuilder._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

@deprecated(message = "don't use", since = "now")
class SimilarPersonsBuilder(
    placeWeight: Double,
    categoryWeight: Double,
    kNearest: Int
) {
  require(placeWeight > 0 && placeWeight < 1.0, s"Place weight must be in the interval (0; 1): $placeWeight")
  require(categoryWeight > 0 && categoryWeight < 1.0, s"Category weight must be in the interval (0; 1): $categoryWeight")
  require(placeWeight + categoryWeight == 1.0, s"Sum of weights must be 1.0: place: $placeWeight, category: $categoryWeight")
  require(kNearest > 0, "K nearest must be positive")

  def calcSimilarPersons(
      placeRatings: DataFrame,
      categoryRatings: DataFrame
  )(implicit spark: SparkSession): DataFrame = {
    val placeBasedSimilarities = calcPlaceBasedSimilarities(placeRatings)
    val categoryBasedSimilarities = calcCategoryBasedSimilarities(categoryRatings)

    val similarities = calcWeightedSimilarities(placeBasedSimilarities, categoryBasedSimilarities)
    val similarPersons = keepKNearest(similarities)
    similarPersons
  }

  private def calcPlaceBasedSimilarities(placeRatings: DataFrame)(implicit spark: SparkSession): DataFrame = {
    calcSimilarities(
      placeRatings,
      entityIdColumn = "place_id",
      ratingColumn = "place_rating",
      similarityColumn = "place_based_similarity"
    )
  }

  private def calcCategoryBasedSimilarities(categoryRatings: DataFrame)(implicit spark: SparkSession): DataFrame = {
    calcSimilarities(
      categoryRatings,
      entityIdColumn = "category_id",
      ratingColumn = "category_rating",
      similarityColumn = "category_based_similarity"
    )
  }

  private def calcWeightedSimilarities(placeBasedSimilarities: DataFrame, categoryBasedSimilarities: DataFrame): DataFrame = {
    placeBasedSimilarities
      .join(categoryBasedSimilarities, Seq("person_id", "that_person_id"), "outer")
      .na.fill(0.0, Seq("place_based_similarity", "category_based_similarity"))
      .select(
        col("person_id"),
        col("that_person_id"),
        col("place_based_similarity") * placeWeight +
          col("category_based_similarity") * categoryWeight
          as "similarity"
      )
  }

  private def keepKNearest(similarities: DataFrame): DataFrame = {
    val window = Window.partitionBy("person_id")
      .orderBy(col("similarity").desc)
    similarities
      .withColumn("rn", row_number() over window)
      .where(col("rn") <= kNearest)
      .drop("rn")
  }

}

object SimilarPersonsBuilder {

  private def calcSimilarities(
      ratings: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      similarityColumn: String
  )(implicit spark: SparkSession): DataFrame = {
    val vectorSize: Int = calcRatingVectorSize(ratings.cache(), entityIdColumn)

    val ratingVectors = aggRatingVectors(
      ratings,
      entityIdColumn,
      ratingColumn,
      vectorColumn = "rating_vector",
      vectorSize
    )
      //TODO Check if cache is really needed
      .cache()

    val thatRatingVectors = ratingVectors
      .withColumnRenamed("person_id", "that_person_id")
      .withColumnRenamed("rating_vector", "that_rating_vector")
    val similarityUdf = udf { (vector: SparseVector, thatVector: SparseVector) =>
      cosineSimilarity(vector, thatVector)
    }
    (ratingVectors crossJoin thatRatingVectors)
      .where(col("person_id") =!= col("that_person_id"))
      //TODO This coalesce is added to avoid too many partitions produced by crossJoin. Remove after reimplementing.
      .coalesce(ratings.rdd.getNumPartitions)
      .withColumn(similarityColumn, similarityUdf(col("rating_vector"), col("that_rating_vector")))
      .where(col(similarityColumn) > 0)
      .select(
        "person_id",
        "that_person_id",
        similarityColumn
      )
  }

  import scala.collection.mutable
  //TODO Better name
  type ElemAgg = mutable.TreeSet[Elem]

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

    def toSparceVector(agg: ElemAgg): SparseVector = {
      val (indexes, values) = agg.map(e => (e.index, e.value)).toArray.unzip
      new SparseVector(vectorSize, indexes, values)
    }

    val ratingVectorsRdd: RDD[(Long, SparseVector)] = aggregatedRatingsRdd.mapValues(toSparceVector)

    val ratingVectors = spark.createDataset[(Long, SparseVector)](ratingVectorsRdd)
      .toDF("person_id", vectorColumn)
    ratingVectors
  }

  private def calcRatingVectorSize(ratings: DataFrame, entityIdColumn: String)(implicit spark: SparkSession): Int = {
    import spark.implicits._

    val maxId = ratings
      .select(max(entityIdColumn))
      .as[Long]
      .head()
    val vectorSize = checkedCast(maxId) + 1
    vectorSize
  }

  private def checkedCast(l: Long): Int = {
    if (l.isValidInt)
      l.toInt
    else
      throw new ArithmeticException(s"Index out of Int range: $l")
  }

}
case class Elem(index: Int, value: Double)
object Elem {
  implicit val elemOrdering: Ordering[Elem] = Ordering.by(_.index)
}
