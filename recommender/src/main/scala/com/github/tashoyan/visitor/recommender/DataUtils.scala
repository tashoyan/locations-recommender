package com.github.tashoyan.visitor.recommender

object DataUtils {

  def generateGraphFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "stochastic_graph")
  }

  def generateSimilarPersonsFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "similar_persons")
  }

  def generatePlaceRatingsFileName(regionIds: Seq[Long], dirPath: String): String = {
    generateFileName(regionIds, dirPath, "place_ratings")
  }

  private def generateFileName(regionIds: Seq[Long], dirPath: String, filePrefix: String): String = {
    regionIds
      .sorted
      .distinct
      .map(regId => s"region$regId")
      .mkString(s"$dirPath/${filePrefix}_", "_", "")
  }

}
