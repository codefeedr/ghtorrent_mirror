package org.codefeedr.experimental.enricher

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.experimental.GitHub.EnrichedCommit
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}
import org.codefeedr.stages.TransformStage2

class EnrichCommitStage(stageId: String = "cf_commit")
    extends TransformStage2[Commit, PushEvent, EnrichedCommit](Some(stageId)) {

  override def transform(
      source: DataStream[Commit],
      secondSource: DataStream[PushEvent]): DataStream[EnrichedCommit] = {

    /** Key on the repo name */
    val commitSource = source.keyBy { x =>
      val regex = """repos\/([^\/]*\/[^\/]*)""".r

      regex.findFirstIn(x.url).get.replace("repos/", "")
    }

    /** Key on the repo name */
    val pushSource = secondSource.keyBy(_.repo.name)

    /** Connect and process a low-level join */
    val processed = pushSource
      .connect(commitSource)
      .process(new EnrichCommitProcess())

    processed
  }
}
