package com.github.tashoyan.visitor.recommender

object DataUtils {

  def generateGraphFileName(regionIds: Seq[Long], dirPath: String): String = {
    regionIds
      .sorted
      .distinct
      .map(regId => s"region$regId")
      .mkString(s"$dirPath/stochastic_graph_", "_", "")
  }

}
