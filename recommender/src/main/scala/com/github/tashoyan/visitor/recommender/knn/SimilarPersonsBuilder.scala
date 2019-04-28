package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.knn.Distance.cosineSimilarity
import com.github.tashoyan.visitor.recommender.knn.SimilarPersonsBuilder._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class SimilarPersonsBuilder(
    placeWeight: Double,
    categoryWeight: Double,
    kNearest: Int
) {
  require(placeWeight > 0 && placeWeight < 1.0, s"Place weight must be in the interval (0; 1): $placeWeight")
  require(categoryWeight > 0 && categoryWeight < 1.0, s"Category weight must be in the interval (0; 1): $categoryWeight")
  require(placeWeight + categoryWeight == 1.0, s"Sum of weights must be 1.0: place: $placeWeight, category: $categoryWeight")
  require(kNearest > 0, "K nearest must be positive")

  private val visitedPlacesTopN: Int = 100
  private val visitedCategoriesTopN: Int = 10

  def calcSimilarPersons(placeVisits: DataFrame): DataFrame = {
    val placeRatings = calcPlaceRatings(placeVisits)
    val placeBasedSimilarities = calcPlaceBasedSimilarities(placeRatings)

    val categoryRatings = calcCategoryRatings(placeVisits)
    val categoryBasedSimilarities = calcCategoryBasedSimilarities(categoryRatings)

    val similarities = calcWeightedSimilarities(placeBasedSimilarities, categoryBasedSimilarities)
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

  private def calcPlaceBasedSimilarities(placeRatings: DataFrame): DataFrame = {
    calcSimilarities(
      placeRatings,
      entityIdColumn = "place_id",
      ratingColumn = "place_rating",
      similarityColumn = "place_based_similarity"
    )
  }

  private def calcCategoryBasedSimilarities(categoryRatings: DataFrame): DataFrame = {
    calcSimilarities(
      categoryRatings,
      entityIdColumn = "category_id",
      ratingColumn = "category_rating",
      similarityColumn = "category_based_similarity"
    )
  }

  private def calcWeightedSimilarities(placeBasedSimilarities: DataFrame, categoryBasedSimilarities: DataFrame): DataFrame = {
    placeBasedSimilarities
      .join(categoryBasedSimilarities, Seq("person_id", "that_person_id"))
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
    val maxId = ratings
      .cache()
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

  private def calcSimilarities(
      ratings: DataFrame,
      entityIdColumn: String,
      ratingColumn: String,
      similarityColumn: String
  ): DataFrame = {
    val ratingVectors = calcRatingVectors(
      ratings,
      entityIdColumn,
      ratingColumn,
      vectorColumn = "rating_vector"
    )

    val thatRatingVectors = ratingVectors
      .withColumnRenamed("person_id", "that_person_id")
      .withColumnRenamed("rating_vector", "that_rating_vector")
    val similarityUdf = udf { (vector: SparseVector, thatVector: SparseVector) =>
      cosineSimilarity(vector, thatVector)
    }
    (ratingVectors crossJoin thatRatingVectors)
      .where(col("person_id") =!= col("that_person_id"))
      .withColumn(similarityColumn, similarityUdf(col("rating_vector"), col("that_rating_vector")))
      .where(col(similarityColumn) > 0)
      .select(
        "person_id",
        "that_person_id",
        similarityColumn
      )
  }

}
