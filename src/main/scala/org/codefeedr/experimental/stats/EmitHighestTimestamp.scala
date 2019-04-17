package org.codefeedr.experimental.stats

import org.apache.flink.api.common.functions.AggregateFunction

import org.codefeedr.experimental.stats.StatsObjects.{CommitStats, Stats}

/** Simple aggregator which only saves the latest timestamp. */
class EmitHighestTimestamp
    extends AggregateFunction[(Long, Stats), (Long, Stats), Stats] {

  /** Creates empty accumulator with lowest timestamp. */
  override def createAccumulator(): (Long, Stats) =
    (0, Stats("", CommitStats(0L, List())))

  /** 'Adds' by picking the one with highest timestamp. */
  override def add(value: (Long, Stats),
                   accumulator: (Long, Stats)): (Long, Stats) =
    if (value._1 > accumulator._1) value else accumulator

  /** Gets the final stats object. */
  override def getResult(accumulator: (Long, Stats)): Stats = accumulator._2

  /** Merges two aggregators by picking the one with highest timestamp. */
  override def merge(a: (Long, Stats), b: (Long, Stats)): (Long, Stats) =
    if (a._1 > b._1) a else b
}
