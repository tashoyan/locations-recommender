package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.linalg.BLAS.dot

object Distance {

  def cosineSimilarity(v1: SparseVector, v2: SparseVector): Double = {
    ???
    //Use BLAS.dot
  }

  def vectorLength(vector: SparseVector): Double = {
    val sumSquared = vector.values
      .map(v => v * v)
      .sum
    math.sqrt(sumSquared)
  }

}
