package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}

class KnnRecommender(
    similarPersons: DataFrame,
    placeRatings: DataFrame
) {

  def makeRecommendations(personId: Long): DataFrame = {
    val personSimilarPersons = similarPersons
      .where(col("person_id") === personId)
    if (isPersonExist(personSimilarPersons))
      makeRecommendations0(personId, personSimilarPersons)
    else
      throw new IllegalArgumentException(s"No such person: $personId")
  }

  private def makeRecommendations0(personId: Long, personSimilarPersons: DataFrame): DataFrame = {
    val otherPersonsPlaceRatings = placeRatings
      .where(col("person_id") =!= personId)
      .withColumnRenamed("person_id", "that_person_id")

    val similarPersonPlaceRaitings = otherPersonsPlaceRatings
      .join(personSimilarPersons, "that_person_id")

    val estimatedPlaceRatings = similarPersonPlaceRaitings
      .withColumn("weighted_rating", col("place_rating") * col("similarity"))
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

  private def isPersonExist(personSimilarPersons: DataFrame): Boolean = {
    personSimilarPersons.count() > 0
  }

}
