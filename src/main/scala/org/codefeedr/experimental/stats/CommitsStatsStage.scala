package org.codefeedr.experimental.stats

import java.text.SimpleDateFormat

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.{OutputStage, TransformStage}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, PurgingTrigger}
import org.codefeedr.experimental.stats.StatsObjects.{Stats, StatsFull}

/** Stage which reduces stat
  * s and sends to MongoDB. */
class CommitsStatsStage(name: String = "commit_stats")
    extends TransformStage[Commit, StatsFull](Some(name)) {

  /** Processes the input stream of commits.
    *
    * @param source the source to read from.
    */
  override def transform(source: DataStream[Commit]): DataStream[StatsFull] = {
    // We want to consider the processing time.
    this.getContext.env
      .setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(new KeyOnDate) // key on hourly date.
      .process(new CommitsStatsProcess) // reduce it.
      .keyBy(_._2.date) // key again on this hourly date.
      .timeWindow(Time.minutes(5)) // every 5 minutes.
      .trigger(PurgingTrigger.of(ProcessingTimeTrigger.create())) // we trigger on processing time and purge the window.
      .aggregate(new EmitHighestTimestamp) // and emit the stats object with highest timestamp.
      .map { x =>
      val format = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateString = x.date


      StatsFull(format.parse(dateString), x.commitStats)
    }
      //.addSink( // finally we send this to mongodb.
      //  new InsertAndReplaceMongoSink(
      //    Map("server" -> "mongodb://localhost:27017",
      //        "database" -> "codefeedr",
      //        "collection" -> "commit_stats")))

  }
}

/** Parses date to hourly and key by it. */
class KeyOnDate extends KeySelector[Commit, String] {
  private lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

  override def getKey(value: Commit): String =
    dateFormat.format(value.commit.committer.date)
}
