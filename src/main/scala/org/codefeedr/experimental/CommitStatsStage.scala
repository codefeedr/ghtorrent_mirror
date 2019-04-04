package org.codefeedr.experimental

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.codefeedr.experimental.Stats.Stats
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.TransformStage
import org.apache.flink.api.scala._

class CommitStatsStage extends TransformStage[Commit, Stats] {

  override def transform(source: DataStream[Commit]): DataStream[Stats] = {
    source.rebalance
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
      .print()

    null
  }
}
