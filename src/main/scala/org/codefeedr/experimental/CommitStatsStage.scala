package org.codefeedr.experimental

import org.apache.flink.api.common.functions.{
  AggregateFunction,
  ReduceFunction,
  RichAggregateFunction
}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.streaming.api.windowing.time.Time
import org.codefeedr.experimental.Stats.{ReducedCommit, Stats}
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.TransformStage
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction

import scala.collection.mutable.Map

class CommitStatsStage extends TransformStage[Commit, Stats] {

  val lateOutputTag = OutputTag[Commit]("late-data")
  override def transform(
      source: DataStream[Commit]): DataStream[ReducedCommit] = {
    val trans = source.rebalance
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.hours(1)) {
          override def extractTimestamp(element: Commit): Long =
            element.commit.committer.date.getTime
        })
      .keyBy { x =>
        val split = x.url.split("/")
        val user = split(4)
        val repo = split(5)

        (user, repo)
      }
      .timeWindow(Time.days(1))
      .allowedLateness(Time.hours(1))
      .sideOutputLateData(lateOutputTag)
      .aggregate(new MinimizeCommit)

    trans.getSideOutput(lateOutputTag).print()

    trans
  }
}

class MinimizeCommit
    extends RichAggregateFunction[Commit, ReducedCommit, ReducedCommit] {

  override def createAccumulator(): ReducedCommit = {
    ReducedCommit("", "", 0, 0, 0, 0, 0, Map.empty[String, Map[String, Int]])
  }

  override def add(value: Commit, accumulator: ReducedCommit): ReducedCommit = {
    if (value.stats.isEmpty) return accumulator //if there are no stats we ignore this commit

    // the accumulator is still empty
    if (accumulator.repo == "" && accumulator.user == "") {
      val split = value.url.split("/")
      val user = split(4)
      val repo = split(5)

      val additions = value.stats.get.additions
      val deletions = value.stats.get.deletions
      val mergedFiles =
        mergeFiles(0, 0, 0, Map.empty[String, Map[String, Int]], value)

      return ReducedCommit(user,
                           repo,
                           additions,
                           deletions,
                           mergedFiles._1,
                           mergedFiles._2,
                           mergedFiles._3,
                           mergedFiles._4)
    }

    val newAdditions = accumulator.totalAdditions + value.stats.get.additions
    val newDeletions = accumulator.totalDeletions + value.stats.get.deletions
    val mergedFiles = mergeFiles(accumulator.filesAdded,
                                 accumulator.filesRemoved,
                                 accumulator.filesModified,
                                 accumulator.filesEdited,
                                 value)

    return ReducedCommit(accumulator.user,
                         accumulator.repo,
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
        }
      }
    }

    (filesAdded, filesModified, filesRemoved, fileMap)
  }

  override def getResult(accumulator: ReducedCommit): ReducedCommit =
    accumulator

  override def merge(a: ReducedCommit, b: ReducedCommit): ReducedCommit = {
    if (a.repo != b.repo || a.user != b.repo) {
      throw new RuntimeException(
        s"Excepted a ReducedCommit from the same repository but instead got two different: ${a.user}/${a.repo} and ${b.user}/${b.repo}")
    }

    val newAdditions = a.totalAdditions + b.totalAdditions
    val newDeletions = a.totalDeletions + b.totalDeletions
    val newFilesAdded = a.filesAdded + b.filesAdded
    val newFilesModified = a.filesModified + b.filesModified
  }
}
