/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.intro

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

case class MatchData(id1: Int, id2: Int,
  scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val rawblocks = sc.textFile(args(0))
    def isHeader(line: String) = line.contains("id_1")
    
    val noheader = rawblocks.filter(x => !isHeader(x))
    def toDouble(s: String) = {
     if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = noheader.map(line => parse(line))
    parsed.cache()

    val matchCounts = parsed.map(md => md.matched).countByValue()
    val matchCountsSeq = matchCounts.toSeq
    // Print counts of matched(true) and unmatched(false) records.
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    val stats = (0 until 9).map(i => {
      parsed.map(_.scores(i)).filter(!_.isNaN).stats()
    })
    // For each of the 9 columns of scores, filter out NaN, feed the remaining
    // to stats(). Then print them all, one record per line.
    stats.foreach(println)

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })
    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    // Print NAStatCounter for all the columns of scores, one record per line.
    // Since NAStatCounter is largely a wrapper around StatCounter, the results
    // shall agree with the above output.
    reduced.foreach(println)

    // For all the score columns, compute StatCounters for the matched and the
    // unmatched records, for which calculate the sum of the number of NaN
    // data points and difference of means. Finally print them, one column per
    // line.
    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
    statsm.zip(statsn).map { case(m, n) =>
      (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    // For all records, compute a special score, which is sum of the scores at
    // 3rd, 6th, 7th, 8th, 9th columns. Filter the score by >=4.0, count and
    // print the number of matched and unmatched records.
    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    // Set threshold to >=2.0, do it again.
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    // An efficient way of computing NAStatCounter for all data columns
    // based on mapPartitions().
    // 1. Within every partition, construct an array of NAStatCounter (of
    //    the same dimension as that of the array in rdd), with ith element
    //    result of adding each values in the ith element in all elements in
    //    the partition. The resulting rdd will has one element (of type
    //    Array[NAStatCounter]) in each partition.
    // 2. Reduce by merging all NAStatCounter arrays and return the final
    //    Array[NAStatCounter].
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      // Has to protect against empty iterators (e.g., small input); see also:
      // http://stackoverflow.com/questions/31605440/how-to-use-mappartitions-in-scala
      // http://stackoverflow.com/questions/31278958/apache-spark-iterating-through-rdd-gives-error-using-mappartitionstopair
      if (iter.hasNext) {
        val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
        iter.foreach(arr => {
          nas.zip(arr).foreach { case (n, d) => n.add(d) }
        })
        Iterator(nas) // same as List(nas).iterator
      } else {
        Iterator.empty
      }
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
