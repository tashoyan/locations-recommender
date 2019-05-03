package com.github.tashoyan.visitor.deduplicator

import scala.math.min

/**
  * [[https://en.wikipedia.org/wiki/Levenshtein_distance Levenshtein distance]].
  */
object Levenshtein {

  /**
    * Calculates Levenshtein distance between two strings.
    * @param str1 The first string.
    * @param str2 The second string.
    * @return The distance between the two strings.
    */
  def lev(str1: String, str2: String): Int = {
    val len1 = str1.length
    val len2 = str2.length
    /* d(i, j) - distance between i chars of str1 and j chars of str2 */
    val d: Array[Array[Int]] = Array.fill(len1 + 1)(new Array[Int](len2 + 1))

    d(0)(0) = 0

    var i = 1
    while (i <= len1) {
      d(i)(0) = i
      i += 1
    }

    var j = 1
    while (j <= len2) {
      d(0)(j) = j
      j += 1
    }

    i = 1
    while (i <= len1) {
      j = 1
      while (j <= len2) {
        val replaceCost =
          if (str1(i - 1) == str2(j - 1))
            0
          else
            1
        val dAddChar = d(i - 1)(j) + 1
        val dRemoveChar = d(i)(j - 1) + 1
        val dReplaceChar = d(i - 1)(j - 1) + replaceCost
        d(i)(j) = min(dAddChar, min(dRemoveChar, dReplaceChar))
        j += 1
      }
      i += 1
    }

    d(len1)(len2)
  }

}
