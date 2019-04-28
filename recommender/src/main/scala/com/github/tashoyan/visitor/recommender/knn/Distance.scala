package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.ml.linalg.SparseVector
import com.github.tashoyan.visitor.recommender.knn.Vectors.RichSparseVector

object Distance {

  def cosineSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
    (vector1 dot vector2) / (vectorLength(vector1) * vectorLength(vector2))
  }

  def vectorLength(vector: SparseVector): Double = {
    val sumSquared = vector.values
      .map(v => v * v)
      .sum
    math.sqrt(sumSquared.toDouble)
  }

}
