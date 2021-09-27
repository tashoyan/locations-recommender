package com.github.tashoyan.recommender.deduplicator

import com.github.tashoyan.recommender.deduplicator.Levenshtein._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LevenshteinTest extends AnyFunSuite with Matchers {

  test("lev - both non-empty strings") {
    val result = lev("sitting", "kitten")
    result should be(3)
  }

  test("lev - first string is empty") {
    val result = lev("", "kitten")
    result should be(6)
  }

  test("lev - second string is empty") {
    val result = lev("sitting", "")
    result should be(7)
  }

  test("lev - both empty strings ") {
    val result = lev("", "")
    result should be(0)
  }

  test("lev - same non-empty string") {
    val result = lev("sitting", "sitting")
    result should be(0)
  }

  test("lev - first string is null") {
    intercept[NullPointerException] {
      lev(null, "sitting")
    }
  }

  test("lev - seconf string is null") {
    intercept[NullPointerException] {
      lev("sitting", null)
    }
  }

}
