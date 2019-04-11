package org.codefeedr.experimental

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.codefeedr.experimental.GitHub.EnrichedCommit
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}
import org.codefeedr.stages.TransformStage2
import org.apache.flink.api.scala._

class EnrichCommitStage
    extends TransformStage2[Commit, PushEvent, EnrichedCommit] {

  val unclassifiedTag = OutputTag[UnclassifiedCommit]("unclassified-data")

  override def transform(
      source: DataStream[Commit],
      secondSource: DataStream[PushEvent]): DataStream[EnrichedCommit] = {
    source.keyBy { x =>
      val regex = """repos\/([^\/]*\/[^\/]*)""".r

      regex.findFirstIn(x.url).get.replace("repos/", "")
    }

    secondSource.keyBy(_.repo.name)

    secondSource
      .connect(source)
      .process(new EnrichCommitProcess(unclassifiedTag))
  }
}
