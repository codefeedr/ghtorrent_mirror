package org.codefeedr.experimental

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.experimental.GitHub.EnrichedCommit
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}
import org.codefeedr.stages.TransformStage2

class EnrichCommitStage
    extends TransformStage2[Commit, PushEvent, EnrichedCommit] {

  override def transform(
      source: DataStream[Commit],
      secondSource: DataStream[PushEvent]): DataStream[EnrichedCommit] = {
    source.keyBy()

    secondSource.keyBy()
  }
}
