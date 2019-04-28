package com.github.tashoyan.visitor.recommender.knn

import breeze.linalg.SparseVector
import com.github.tashoyan.visitor.recommender.knn.Distance._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class DistanceTest extends FunSuite {

  test("vectorLength - all vector components are zero") {
    val vector = new SparseVector[Long](Array.emptyIntArray, Array.emptyLongArray, 2)
    val result = vectorLength(vector)
    result should be(0.0)
  }

  test("vectorLength - one vector component is non-zero") {
    val vector = new SparseVector[Long](Array(0), Array(1L), 2)
    val result = vectorLength(vector)
    result should be(1.0)
  }

  test("vectorLength - all vector components are non-zero") {
    val vector = new SparseVector[Long](Array(0, 1), Array(3L, 4L), 2)
    val result = vectorLength(vector)
    result should be(5.0)
  }

  test("vectorLength - all vector components are negative") {
    val vector = new SparseVector[Long](Array(0, 1), Array(-3L, -4L), 2)
    val result = vectorLength(vector)
    result should be(5.0)
  }

  test("cosineSimilarity - collinear vectors, same direction") {
    val vector1 = new SparseVector[Long](Array(0), Array(2L), 2)
    val vector2 = new SparseVector[Long](Array(0), Array(3L), 2)
    val result = cosineSimilarity(vector1, vector2)
    result should be(1.0)
  }

  test("cosineSimilarity - collinear vectors, opposite direction") {
    val vector1 = new SparseVector[Long](Array(0), Array(2L), 2)
    val vector2 = new SparseVector[Long](Array(0), Array(-3L), 2)
    val result = cosineSimilarity(vector1, vector2)
    result should be(-1.0)
  }

  test("cosineSimilarity - orthogonal vectors") {
    val vector1 = new SparseVector[Long](Array(0), Array(2L), 2)
    val vector2 = new SparseVector[Long](Array(1), Array(3L), 2)
    val result = cosineSimilarity(vector1, vector2)
    result should be(0.0)
  }

  test("cosineSimilarity - vectors at 45 degrees") {
    val vector1 = new SparseVector[Long](Array(0), Array(2L), 2)
    val vector2 = new SparseVector[Long](Array(0, 1), Array(1L, 1L), 2)
    val result = cosineSimilarity(vector1, vector2)
    result should be(1 / math.sqrt(2))
  }

}
