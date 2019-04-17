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

  /** Count all the commits processed. */
  private val sumCommits = new LongCounter()

  /** Open this ProcessFunction by creating an accumulator and create state.
    *
    * @param parameters the configuration.
    */
  override def open(parameters: Configuration): Unit = {
    val statsStateDescriptor =
      new ValueStateDescriptor[Stats]("stats", classOf[Stats])
    statsState = getRuntimeContext.getState[Stats](statsStateDescriptor)

    getRuntimeContext.addAccumulator("total_commits_processed", sumCommits)
  }

  /** Process an element and reduce to a stats object.
    *
    * @param value the commit to reduce.
    * @param ctx the context.
    * @param out the collector.
    */
  override def processElement(
      value: Commit,
      ctx: KeyedProcessFunction[String, Commit, (Long, Stats)]#Context,
      out: Collector[(Long, Stats)]): Unit = {
    val currentStats = statsState.value()

    // If there is no state, create it.
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

    // Increment accumulator.
    sumCommits.add(1)
  }

  /** Creates a stats object based on a Commit.
    *
    * @param key the key (which is the date).
    * @param commit the commit to create it from.
    * @return a Stats object.
    */
  def createStats(key: String, commit: Commit): Stats = {
    val date = key

    // For all files merge an extension list.
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

  /** Merges two extensions.
    *
    * @param a the first extension to merge.
    * @param b the second extension to merge.
    * @return the merged extension.
    */
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

  /** Merges two stats objects into one.
    *
    * @param statsA the first stats object.
    * @param statsB the second stats object.
    * @return A merged stats object.
    */
  def merge(statsA: Stats, statsB: Stats): Stats = {
    val extensionsA = statsA.commitStats.filesEdited
    val extensionsB = statsB.commitStats.filesEdited

    // FP magic!
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
