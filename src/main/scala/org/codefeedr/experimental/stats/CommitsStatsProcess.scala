package org.codefeedr.experimental.stats

import java.text.SimpleDateFormat

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.codefeedr.experimental.stats.StatsObjects.{
  CommitStats,
  Extension,
  Stats
}
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

class CommitsStatsProcess
    extends KeyedProcessFunction[String, Commit, (Long, Stats)] {
  private var statsState: ValueState[Stats] = _

  private val sumCommits = new LongCounter()

  override def open(parameters: Configuration): Unit = {
    val statsStateDescriptor =
      new ValueStateDescriptor[Stats]("stats", classOf[Stats])
    statsState = getRuntimeContext.getState[Stats](statsStateDescriptor)

    getRuntimeContext.addAccumulator("total_commits_processed", sumCommits)
  }

  override def processElement(
      value: Commit,
      ctx: KeyedProcessFunction[String, Commit, (Long, Stats)]#Context,
      out: Collector[(Long, Stats)]): Unit = {
    val currentStats = statsState.value()

    if (currentStats == null) {
      val stats = createStats(ctx.getCurrentKey, value)

      //Let's collect.
      statsState.update(stats)
      out.collect(ctx.timerService().currentProcessingTime(),
                  createStats(ctx.getCurrentKey, value))
    } else {
      // Update stats and state.
      val newStats = merge(createStats(ctx.getCurrentKey, value), currentStats)
      statsState.update(newStats)

      // Let's collect.
      out.collect(ctx.timerService().currentProcessingTime(), newStats)
    }

    sumCommits.add(1)
  }

  def createStats(key: String, commit: Commit): Stats = {
    val date = key

    val extensions = commit.files
      .map { file =>
        var extension = "unknown"

        if (file.filename.isDefined) {
          val pattern = "\\.[0-9a-z]+$".r
          extension = pattern
            .findFirstIn(file.filename.get)
            .getOrElse("unknown")
            .replace(".", "")
        }

        val additions = file.additions
        val deletions = file.deletions
        var added = 0
        var removed = 0
        var modified = 0

        file.status match {
          case Some("added")    => added = 1
          case Some("removed")  => removed = 1
          case Some("modified") => modified = 1
          case _                =>
        }

        Extension(extension, additions, deletions, added, removed, modified)
      }
      .groupBy(_.name)
      .mapValues(_.reduce(mergeExtension))
      .map(_._2)
      .toList

    Stats(date, CommitStats(1, extensions))
  }

  def mergeExtension(a: Extension, b: Extension): Extension = (a, b) match {
    case (Extension(nameA, addiA, deliA, addA, remA, modA),
          Extension(nameB, addiB, deliB, addB, remB, modB)) if nameA == nameB =>
      Extension(nameA,
                addiA + addiB,
                deliA + deliB,
                addA + addB,
                remA + remB,
                modA + modB)
    case _ =>
      throw new RuntimeException(
        "Cannot merge two extensions because they have different names.")
  }

  def merge(statsA: Stats, statsB: Stats): Stats = {
    val extensionsA = statsA.commitStats.filesEdited
    val extensionsB = statsB.commitStats.filesEdited

    val extensions = (extensionsA ::: extensionsB)
      .groupBy(_.name)
      .mapValues(_.reduce(mergeExtension))
      .map(_._2)
      .toList

    Stats(statsA.date,
          CommitStats(
            statsA.commitStats.totalCommits + statsB.commitStats.totalCommits,
            extensions))
  }

}
