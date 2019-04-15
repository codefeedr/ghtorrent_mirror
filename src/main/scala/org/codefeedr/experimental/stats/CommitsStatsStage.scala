package org.codefeedr.experimental.stats

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.OutputStage
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{
  ProcessingTimeTrigger,
  PurgingTrigger
}
import org.codefeedr.experimental.stats.StatsObjects.Stats

class CommitsStatsStage(name: String = "commit_stats")
    extends OutputStage[Commit](Some(name)) {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

  override def main(source: DataStream[Commit]): Unit = {
    // We wan't to consider the processing time.
    this.getContext.env
      .setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(x => dateFormat.format(x.commit.committer.date))
      .process(new CommitsStatsProcess)
      .keyBy(_._2.date)
      .timeWindow(Time.minutes(10))
      .trigger(PurgingTrigger.of(new ProcessingTimeTrigger()))
      .process(new EmitHighestTimestamp)
      .print()

  }
}
