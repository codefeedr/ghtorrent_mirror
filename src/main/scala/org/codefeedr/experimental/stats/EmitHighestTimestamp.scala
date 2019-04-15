package org.codefeedr.experimental.stats

import org.apache.flink.api.common.functions.AggregateFunction

import org.codefeedr.experimental.stats.StatsObjects.{CommitStats, Stats}

class EmitHighestTimestamp
    extends AggregateFunction[(Long, Stats), (Long, Stats), Stats] {
  override def createAccumulator(): (Long, Stats) =
    (0, Stats("", CommitStats(0L, List())))

  override def add(value: (Long, Stats),
                   accumulator: (Long, Stats)): (Long, Stats) =
    if (value._1 > accumulator._1) value else accumulator

  override def getResult(accumulator: (Long, Stats)): Stats = accumulator._2

  override def merge(a: (Long, Stats), b: (Long, Stats)): (Long, Stats) =
    if (a._1 > b._1) a else b
}
