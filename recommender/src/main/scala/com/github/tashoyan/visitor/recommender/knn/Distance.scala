package com.github.tashoyan.visitor.recommender.knn

import breeze.linalg.SparseVector

object Distance {

  def cosineSimilarity(vector1: SparseVector[Long], vector2: SparseVector[Long]): Double = {
    (vector1 dot vector2) / (vectorLength(vector1) * vectorLength(vector2))
  }

  def vectorLength(vector: SparseVector[Long]): Double = {
    val sumSquared = vector.data
      .map(v => v * v)
      .sum
    math.sqrt(sumSquared.toDouble)
  }

}
