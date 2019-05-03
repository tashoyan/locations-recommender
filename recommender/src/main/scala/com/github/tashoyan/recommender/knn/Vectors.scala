package com.github.tashoyan.recommender.knn

import breeze.linalg.{SparseVector => BSV}
import org.apache.spark.ml.linalg.SparseVector

object Vectors {

  implicit class RichSparseVector(val vector: SparseVector) extends AnyVal {

    @inline def toBreeze: BSV[Double] =
      new BSV[Double](vector.indices, vector.values, vector.size)

    @inline def dot(that: SparseVector): Double =
      this.toBreeze dot that.toBreeze

  }

}
