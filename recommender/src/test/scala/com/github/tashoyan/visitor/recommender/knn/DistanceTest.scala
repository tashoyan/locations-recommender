package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.knn.Distance._
import org.apache.spark.ml.linalg.SparseVector
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class DistanceTest extends FunSuite {

  test("vectorLength - all vector components are zero") {
    val vector = new SparseVector(2, Array.emptyIntArray, Array.emptyDoubleArray)
    val result = vectorLength(vector)
    result should be(0.0)
  }

  test("vectorLength - one vector component is non-zero") {
    val vector = new SparseVector(2, Array(0), Array(1.0))
    val result = vectorLength(vector)
    result should be(1.0)
  }

  test("vectorLength - all vector components are non-zero") {
    val vector = new SparseVector(2, Array(0, 1), Array(3.0, 4.0))
    val result = vectorLength(vector)
    result should be(5.0)
  }

  test("vectorLength - all vector components are negative") {
    val vector = new SparseVector(2, Array(0, 1), Array(-3.0, -4.0))
    val result = vectorLength(vector)
    result should be(5.0)
  }

}
