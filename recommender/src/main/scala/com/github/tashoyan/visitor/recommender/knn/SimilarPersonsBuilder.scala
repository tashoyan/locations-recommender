package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.knn.SimilarPersonsBuilder._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class SimilarPersonsBuilder(
    alphaPlace: Double,
    alphaCategory: Double,
    kNearest: Int
) {
  require(alphaPlace > 0 && alphaPlace < 1.0, s"alphaPlace must be in the interval (0; 1): $alphaPlace")
  require(alphaCategory > 0 && alphaCategory < 1.0, s"alphaCategory must be in the interval (0; 1): $alphaCategory")
  require(alphaPlace + alphaCategory == 1.0, s"Sum of coefficients must be 1.0: alphaPlace: $alphaPlace, alphaCategory: $alphaCategory")
  require(kNearest > 0, "K nearest must be positive")

  private val visitedPlacesTopN: Int = 100
  private val visitedCategoriesTopN: Int = 100

  def calcSimilarPersons(placeVisits: DataFrame): DataFrame = {
    val placeRatings = calcPlaceRatings(placeVisits)
    val placeBasedVectors = calcPlaceBasedVectors(placeRatings)
    placeBasedVectors.show(false)
    val placeBasedSimilarities = calcPlaceBasedSimilarities(placeBasedVectors)

    val categoryRatings = calcCategoryRatings(placeVisits)
    val categoryBasedVectors = calcCategoryBasedVectors(categoryRatings)
    categoryBasedVectors.show(false)
    val categoryBasedSimilarities = calcCategoryBasedSimilarities(categoryBasedVectors)

    val similarities = calcSimilarities(placeBasedSimilarities, categoryBasedSimilarities)
    keepKNearest(similarities)
  }

  private def calcPlaceRatings(placeVisits: DataFrame): DataFrame = {
    calcRatings(
      placeVisits,
      entityIdColumn = "place_id",
      ratingColumn = "place_rating",
      topN = visitedPlacesTopN
    )
  }

  private def calcCategoryRatings(placeVisits: DataFrame): DataFrame = {
    calcRatings(
      placeVisits,
      entityIdColumn = "category_id",
      ratingColumn = "category_rating",
      topN = visitedCategoriesTopN
    )
  }

  private def calcPlaceBasedVectors(placeRatings: DataFrame): DataFrame = {
    calcRatingVectors(
      placeRatings,
      entityIdColumn = "place_id",
      ratingColumn = "place_rating",
      vectorColumn = "place_rating_vector"
    )
  }

  private def calcCategoryBasedVectors(categoryRatings: DataFrame): DataFrame = {
    calcRatingVectors(
      categoryRatings,
      entityIdColumn = "category_id",
      ratingColumn = "category_rating",
      vectorColumn = "category_rating_vector"
    )
  }

  private def calcPlaceBasedSimilarities(placeBasedVectors: DataFrame): DataFrame = {
    val thatVectors = placeBasedVectors
      .withColumnRenamed("person_id", "that_person_id")
    (placeBasedVectors crossJoin thatVectors)
      //TODO Implement
      .withColumn("place_based_similarity", lit(0.5))
      .where(col("place_based_similarity") > 0)
      .select(
        "person_id",
        "that_person_id",
        "place_based_similarity"
      )
  }

  private def calcCategoryBasedSimilarities(categoryBasedVectors: DataFrame): DataFrame = {
    val thatVectors = categoryBasedVectors
      .withColumnRenamed("person_id", "that_person_id")
    (categoryBasedVectors crossJoin thatVectors)
      //TODO Implement
      .withColumn("category_based_similarity", lit(0.5))
      .where(col("category_based_similarity") > 0)
      .select(
        "person_id",
        "that_person_id",
        "category_based_similarity"
      )
  }

  private def calcSimilarities(placeBasedSimilarities: DataFrame, categoryBasedSimilarities: DataFrame): DataFrame = {
    placeBasedSimilarities
      .join(categoryBasedSimilarities, Seq("person_id", "that_person_id"))
      .select(
        col("person_id"),
        col("that_person_id"),
        col("place_based_similarity") * alphaPlace +
          col("category_based_similarity") * alphaCategory
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

  private def calcRatings(
      placeVisits: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      topN: Int
  ): DataFrame = {
    val personVisitCounts = placeVisits
      .groupBy("person_id", entityIdColumn)
      .agg(count("*") as ratingColumn)

    val window = Window.partitionBy("person_id")
      .orderBy(col(ratingColumn).desc)
    personVisitCounts
      .withColumn("rank", rank() over window)
      .where(col("rank") <= topN)
      .drop("rank")
  }

  private def calcRatingVectors(
      ratings: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      vectorColumn: String
  ): DataFrame = {
    //TODO ratings must be cached
    val maxId = ratings
      .select(max(entityIdColumn))
      .head()
      .getLong(0)
      .toInt
    val vectorSize = maxId + 1

    val createVectorUdf = udf { (indexes: Seq[Long], values: Seq[Long]) =>
      new SparseVector(
        size = vectorSize,
        indices = indexes
          .map(_.toInt)
          .toArray,
        values = values
          .map(_.toDouble)
          .toArray
      )
    }
    //TODO It can be done more efficiently with a custom aggregation function: collect_sparse_vector()
    val vectors = ratings
      .orderBy("person_id", entityIdColumn)
      .groupBy("person_id")
      .agg(
        collect_list(entityIdColumn) as "indexes",
        collect_list(ratingColumn) as "values"
      )
      .select(
        col("person_id"),
        createVectorUdf(col("indexes"), col("values")) as vectorColumn
      )
    vectors
  }

}
