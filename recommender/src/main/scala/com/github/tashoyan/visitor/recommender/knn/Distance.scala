package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.ml.linalg.SparseVector

object Distance {

  def cosineSimilarity(v1: SparseVector, v2: SparseVector): Double = {
    ???
    //Use BLAS.dot
  }

}
