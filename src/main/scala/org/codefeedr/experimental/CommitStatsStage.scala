package org.codefeedr.experimental

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Date

import org.apache.flink.api.common.functions.{
  AggregateFunction,
  ReduceFunction,
  RichMapFunction,
  RichReduceFunction
}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.codefeedr.experimental.StatsObjects._
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.TransformStage
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.Map

class CommitStatsStage(stageName: String = "daily_commits_stats")
    extends TransformStage[Commit, Stats](Some(stageName)) {

  val lateOutputTag = OutputTag[Commit]("late-data")
  override def transform(source: DataStream[Commit]): DataStream[Stats] = {
    val trans = source.rebalance
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Commit](Time.days(31)) {
          override def extractTimestamp(element: Commit): Long =
            element.commit.committer.date.getTime
        })
      .map(new PartitionKeyMap)
      .keyBy(0)
      .timeWindow(Time.days(1))
      .allowedLateness(Time.days(31))
      .trigger(CountTrigger.of(1000))
      .aggregate(new MinimizeCommit, new ProcessCommitWindow)
      .keyBy(_.date)
      .reduce(new ReduceStats)

    /**
    trans
      .getSideOutput(lateOutputTag)
      .map(x => (x.commit.committer.date, x.commit.author.date))
      .print()
      */
    trans
  }
}
class ProcessCommitWindow
    extends ProcessWindowFunction[ReducedCommits, Stats, Tuple, TimeWindow] {
  override def process(key: Tuple,
                       context: Context,
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
    extends AggregateFunction[(Int, Commit), ReducedCommits, ReducedCommits] {

  override def createAccumulator(): ReducedCommits =
    ReducedCommits(0, 0, 0, 0, 0, 0, List())

  override def add(value: (Int, Commit),
                   accumulator: ReducedCommits): ReducedCommits = {
    if (value._2.stats.isEmpty) return accumulator //if there are no stats we ignore this commit

    val newAdditions = accumulator.totalAdditions + value._2.stats.get.additions
    val newDeletions = accumulator.totalDeletions + value._2.stats.get.deletions
    val mergedFiles = mergeFiles(accumulator.filesAdded,
                                 accumulator.filesRemoved,
                                 accumulator.filesModified,
                                 accumulator.filesEdited,
                                 value._2)
    val newCommits = accumulator.totalCommits + 1

    return ReducedCommits(newCommits,
                          newAdditions,
                          newDeletions,
                          mergedFiles._1,
                          mergedFiles._2,
                          mergedFiles._3,
                          mergedFiles._4)
  }

  def mergeFiles(added: Long,
                 removed: Long,
                 modified: Long,
                 fileMap: List[Extension],
                 value: Commit): (Long, Long, Long, List[Extension]) = {

    var filesAdded = added
    var filesRemoved = removed
    var filesModified = modified

    var newExtenionList = fileMap

    value.files.foreach { file =>
      var extension = "unknown"

      if (file.filename.isDefined) {
        val pattern = "\\.[0-9a-z]+$".r
        extension = pattern
          .findFirstIn(file.filename.get)
          .getOrElse("unknown")
          .replace(".", "")
      }

      // Get extension map.
      val oldExtension = newExtenionList.find(_.name == extension)

      // If it is already defined then update the amount of additions and deletions.
      if (oldExtension.isDefined) {
        val additions = oldExtension.get.additions + file.additions
        val deletions = oldExtension.get.deletions + file.deletions
        var created = oldExtension.get.created

        // There are no file changes, but there is a new file so it has been created.
        if (file.changes == 0) {
          created += 1
        }

        newExtenionList = newExtenionList.filter(_.name != extension)
        newExtenionList = Extension(extension, additions, deletions, created) :: newExtenionList

      } else { // If it is not defined then insert the amount of additions and deletions for that extensions.
        val additions = file.additions.toLong
        val deletions = file.deletions.toLong
        var created = 0

        if (file.changes == 0) {
          created = 1
        }

        newExtenionList = Extension(extension, additions, deletions, created) :: newExtenionList
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

    (filesAdded, filesModified, filesRemoved, newExtenionList)
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
    val newMap = (a.filesEdited ++ b.filesEdited)
      .groupBy(_.name)
      .mapValues(_.foldLeft((0L, 0L, 0L)) {
        case ((add, del, create),
              Extension(name, additions, deletions, created)) =>
          (add + additions, del + deletions, create + created)
      })
      .map {
        case (key, (add, del, create)) => Extension(key, add, del, create)
      }
      .toList

    ReducedCommits(newCommits,
                   newAdditions,
                   newDeletions,
                   newFilesAdded,
                   newFilesModified,
                   newFilesRemoved,
                   newMap)
  }
}

class PartitionKeyMap() extends RichMapFunction[Commit, (Int, Commit)] {
  override def map(value: Commit): (Int, Commit) = {
    (this.getRuntimeContext.getIndexOfThisSubtask, value)
  }
}

class ReduceStats extends ReduceFunction[Stats] {
  override def reduce(value1: Stats, value2: Stats): Stats = {
    if (value1.date != value2.date) {
      throw new RuntimeException("Dates should be the same in order to reduce.")
    }

    val mergedCommits = merge(value1.reducedCommit, value2.reducedCommit)

    Stats(value1.date, mergedCommits)
  }

  def merge(a: ReducedCommits, b: ReducedCommits): ReducedCommits = {
    val newAdditions = a.totalAdditions + b.totalAdditions
    val newDeletions = a.totalDeletions + b.totalDeletions
    val newCommits = a.totalCommits + b.totalCommits
    val newFilesAdded = a.filesAdded + b.filesAdded
    val newFilesModified = a.filesModified + b.filesModified
    val newFilesRemoved = a.filesRemoved + b.filesRemoved
    val newMap = (a.filesEdited ++ b.filesEdited)
      .groupBy(_.name)
      .mapValues(_.foldLeft((0L, 0L, 0L)) {
        case ((add, del, create),
              Extension(name, additions, deletions, created)) =>
          (add + additions, del + deletions, create + created)
      })
      .map {
        case (key, (add, del, create)) => Extension(key, add, del, create)
      }
      .toList

    ReducedCommits(newCommits,
                   newAdditions,
                   newDeletions,
                   newFilesAdded,
                   newFilesModified,
                   newFilesRemoved,
                   newMap)
  }
}
