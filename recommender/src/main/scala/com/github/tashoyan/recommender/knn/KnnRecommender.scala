package com.github.tashoyan.recommender.knn

import com.github.tashoyan.recommender.knn.Distance.cosineSimilarity
import com.github.tashoyan.recommender.knn.KnnRecommender._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum, udf}

class KnnRecommender(
    placeRatingVectors: DataFrame,
    categoryRatingVectors: DataFrame,
    placeRatings: DataFrame,
    placeWeight: Double,
    categoryWeight: Double,
    kNearest: Int
) {
  require(placeWeight > 0 && placeWeight < 1.0, s"Place weight must be in the interval (0; 1): $placeWeight")
  require(categoryWeight > 0 && categoryWeight < 1.0, s"Category weight must be in the interval (0; 1): $categoryWeight")
  require(placeWeight + categoryWeight == 1.0, s"Sum of weights must be 1.0: place: $placeWeight, category: $categoryWeight")
  require(kNearest > 0, "K nearest must be positive")

  def makeRecommendations(personId: Long): DataFrame = {
    val similarPersons = findSimilarPersons(personId)
    makeRecommendations0(personId, similarPersons)
  }

  private def findSimilarPersons(personId: Long): DataFrame = {
    val placeSimilarPersons = findSimilarPersons0(
      personId,
      ratingVectors = placeRatingVectors,
      similarityColumn = "place_similarity"
    )
    val categorySimilarPersons = findSimilarPersons0(
      personId,
      ratingVectors = categoryRatingVectors,
      similarityColumn = "category_similarity"
    )
    placeSimilarPersons
      .join(categorySimilarPersons, Seq("person_id"), "outer")
      .na.fill(0.0, Seq("place_similarity", "category_similarity"))
      .select(
        col("person_id"),
        col("place_similarity") * placeWeight +
          col("category_similarity") * categoryWeight
          as "similarity"
      )
      .orderBy(col("similarity").desc)
      .limit(kNearest)
  }

  private def makeRecommendations0(personId: Long, similarPersons: DataFrame): DataFrame = {
    val otherPersonsPlaceRatings = placeRatings
      .where(col("person_id") =!= personId)

    val similarPersonPlaceRaitings = otherPersonsPlaceRatings
      .join(similarPersons, "person_id")

    val estimatedPlaceRatings = similarPersonPlaceRaitings
      .withColumn("weighted_rating", col("rating") * col("similarity"))
      .groupBy("place_id")
      .agg(
        sum("weighted_rating") as "weighted_rating_sum",
        sum("similarity") as "similarity_sum"
      )
      .select(
        col("place_id"),
        col("weighted_rating_sum") / col("similarity_sum") as "estimated_rating"
      )
    estimatedPlaceRatings
  }

}

object KnnRecommender {

  private def findSimilarPersons0(personId: Long, ratingVectors: DataFrame, similarityColumn: String): DataFrame = {
    val personRatingVector = ratingVectors
      .where(col("person_id") === personId)
      .select("rating_vector")
      .head(1)
      .headOption
      .map(_.getAs[SparseVector]("rating_vector"))
      .getOrElse(throw new IllegalArgumentException(s"No such person: $personId"))

    val similarityUdf = udf { vector: SparseVector =>
      cosineSimilarity(vector, personRatingVector)
    }
    ratingVectors
      .where(col("person_id") =!= personId)
      .withColumn(similarityColumn, similarityUdf(col("rating_vector")))
      .where(col(similarityColumn) > 0)
      .select(
        "person_id",
        similarityColumn
      )
  }

}
