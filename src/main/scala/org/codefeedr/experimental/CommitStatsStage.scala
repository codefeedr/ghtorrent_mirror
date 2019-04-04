package org.codefeedr.experimental

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Date

import org.apache.flink.api.common.functions.{
  AggregateFunction,
  ReduceFunction,
  RichAggregateFunction
}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.streaming.api.windowing.time.Time
import org.codefeedr.experimental.StatsObjects._
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.TransformStage
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.{
  ProcessAllWindowFunction,
  ProcessWindowFunction
}
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.Map

class CommitStatsStage(stageName: String = "daily_commit_stats")
    extends TransformStage[Commit, Stats](Some(stageName)) {

  val lateOutputTag = OutputTag[Commit]("late-data")
  override def transform(source: DataStream[Commit]): DataStream[Stats] = {
    val trans = source.rebalance
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.days(1)) {
          override def extractTimestamp(element: Commit): Long =
            element.commit.committer.date.getTime
        })
      .timeWindowAll(Time.days(1))
      .allowedLateness(Time.days(1))
      .sideOutputLateData(lateOutputTag)
      .trigger(EventTimeTrigger.create())
      .aggregate(new MinimizeCommit, new ProcessCommitWindow)

    trans
      .getSideOutput(lateOutputTag)
      .map(_.commit.committer.date)
      .print()

    trans
  }
}
class ProcessCommitWindow
    extends ProcessAllWindowFunction[ReducedCommits, Stats, TimeWindow] {
  override def process(context: Context,
                       elements: Iterable[ReducedCommits],
                       out: Collector[Stats]): Unit = {
    if (elements.iterator.size > 1) {
      println("The iterator size is bigger than 1. That should be impossible.")
    }

    val reducedCommit = elements.iterator.next()
    val window = Instant.ofEpochMilli(context.window.getStart)

    out.collect(
      Stats(
        window.plus(12, ChronoUnit.HOURS).truncatedTo(ChronoUnit.DAYS).toString,
        reducedCommit))
  }
}
class MinimizeCommit
    extends AggregateFunction[Commit, ReducedCommits, ReducedCommits] {

  override def createAccumulator(): ReducedCommits = {
    ReducedCommits(0, 0, 0, 0, 0, 0, Map.empty[String, Map[String, Int]])
  }

  override def add(value: Commit,
                   accumulator: ReducedCommits): ReducedCommits = {
    if (value.stats.isEmpty) return accumulator //if there are no stats we ignore this commit

    val newAdditions = accumulator.totalAdditions + value.stats.get.additions
    val newDeletions = accumulator.totalDeletions + value.stats.get.deletions
    val mergedFiles = mergeFiles(accumulator.filesAdded,
                                 accumulator.filesRemoved,
                                 accumulator.filesModified,
                                 accumulator.filesEdited,
                                 value)
    val newCommits = accumulator.totalCommits + 1

    return ReducedCommits(newCommits,
                          newAdditions,
                          newDeletions,
                          mergedFiles._1,
                          mergedFiles._2,
                          mergedFiles._3,
                          mergedFiles._4)
  }

  def mergeFiles(
      added: Int,
      removed: Int,
      modified: Int,
      fileMap: Map[String, Map[String, Int]],
      value: Commit): (Int, Int, Int, Map[String, Map[String, Int]]) = {

    var filesAdded = added
    var filesRemoved = removed
    var filesModified = modified

    value.files.foreach { file =>
      var extension = "unknown"

      if (file.filename.isDefined) {
        val pattern = "\\.[0-9a-z]+$".r
        extension = pattern.findFirstIn(file.filename.get).getOrElse("unknown")
      }

      // Get extension map.
      val extensionMap = fileMap.get(extension)

      // If it is already defined then update the amount of additions and deletions.
      if (extensionMap.isDefined) {
        val additions = extensionMap.get
          .get("additions")
          .getOrElse(0) + file.additions
        val deletions = extensionMap.get
          .get("deletions")
          .getOrElse(0) + file.deletions

        extensionMap.get.update("additions", additions)
        extensionMap.get.update("deletions", deletions)
      } else { // If it is not defined then insert the amount of additions and deletions for that extensions.
        val additions = file.additions
        val deletions = file.deletions
        val map = Map("additions" -> additions, "deletions" -> deletions)

        fileMap.update(extension, map)
      }

      // Also save the file status
      if (file.status.isDefined) {
        file.status.get match {
          case "added"    => filesAdded += 1
          case "removed"  => filesRemoved += 1
          case "modified" => filesModified += 1
          case _          =>
        }
      }
    }

    (filesAdded, filesModified, filesRemoved, fileMap)
  }

  override def getResult(accumulator: ReducedCommits): ReducedCommits =
    accumulator

  override def merge(a: ReducedCommits, b: ReducedCommits): ReducedCommits = {
    val newAdditions = a.totalAdditions + b.totalAdditions
    val newDeletions = a.totalDeletions + b.totalDeletions
    val newCommits = a.totalCommits + b.totalCommits
    val newFilesAdded = a.filesAdded + b.filesAdded
    val newFilesModified = a.filesModified + b.filesModified
    val newFilesRemoved = a.filesRemoved + b.filesRemoved
    val newMap = a.filesEdited ++ b.filesEdited

    ReducedCommits(newCommits,
                   newAdditions,
                   newDeletions,
                   newFilesAdded,
                   newFilesModified,
                   newFilesRemoved,
                   newMap)
  }
}
