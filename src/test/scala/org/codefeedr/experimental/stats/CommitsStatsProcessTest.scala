package org.codefeedr.experimental.stats

import org.codefeedr.experimental.stats.StatsObjects.{
  CommitStats,
  Extension,
  Stats
}
import org.scalatest.FunSuite

class CommitsStatsProcessTest extends FunSuite {

  test("Two stats should be properly merged") {
    val ex1 = new Extension("java", 1, 2, 3, 0, 1)
    val ex3 = new Extension("blabla", 1, 2, 3, 0, 1)
    val ex2 = new Extension("java", 2, 3, 4, 0, 0)

    val stats = new Stats("ha", CommitStats(1, List(ex1, ex3)))
    val stats2 = new Stats("ha", CommitStats(1, List(ex2)))
    val process = new CommitsStatsProcess

    val merged =
      Stats("ha", CommitStats(2, List(ex3, Extension("java", 3, 5, 7, 0, 1))))
    assert(process.merge(stats, stats2) == merged)
  }

}
