package org.codefeedr.experimental.stats

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.codefeedr.experimental.stats.StatsObjects.{
  CommitStats,
  Extension,
  Stats
}
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

class CommitsStatsProcess extends ProcessFunction[Commit, (Long, Stats)] {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
  private var statsState: ValueState[Stats] = _

  override def open(parameters: Configuration): Unit = {
    val statsStateDescriptor =
      new ValueStateDescriptor[Stats]("stats", classOf[Stats])
    statsState = getRuntimeContext.getState[Stats](statsStateDescriptor)

  }

  override def processElement(
      value: Commit,
      ctx: ProcessFunction[Commit, (Long, Stats)]#Context,
      out: Collector[(Long, Stats)]): Unit = {
    val currentStats = statsState.value()

    if (currentStats == null) {
      //Let's collect.
      out.collect(ctx.timestamp(), createStats(value))
    } else {
      // Update stats and state.
      val newStats = merge(createStats(value), currentStats)
      statsState.update(newStats)

      // Let's collect.
      out.collect(ctx.timestamp(), createStats(value))
    }

  }

  def createStats(commit: Commit): Stats = {
    val date = dateFormat.format(commit.commit.committer.date)

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
          case Some("created")  => added = 1
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
