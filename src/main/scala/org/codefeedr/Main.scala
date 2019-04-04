package org.codefeedr

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.codefeedr.buffer.KafkaBuffer
import org.codefeedr.experimental.CommitStatsStage
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.plugins.ghtorrent.stages.GHTEventStages._
import org.codefeedr.plugins.ghtorrent.stages.{
  GHTCommitStage,
  GHTInputStage,
  SideOutput
}

object Main {

  // sideoutput configuration
  val sideOutput = SideOutput(true, sideOutputKafkaServer = "localhost:29092")

  // all stages
  val inputStage = new GHTInputStage("wzorgdrager")
  val commitStage = new GHTCommitStage(sideOutput = sideOutput)
  val commitcommentStage = new GHTCommitCommentEventStage(
    sideOutput = sideOutput)
  val pushStage = new GHTPushEventStage(sideOutput = sideOutput)
  val createStage = new GHTCreateEventStage(sideOutput = sideOutput)
  val deleteStage = new GHTDeleteEventStage(sideOutput = sideOutput)

  val deploymentStage = new GHTDeploymentEventStage(sideOutput = sideOutput)
  val deploymentStatusStage = new GHTDeploymentStatusEventStage(
    sideOutput = sideOutput)
  val gollumStage = new GHTGolumEventStage(sideOutput = sideOutput)
  val memberShipStage = new GHTMemberShipEventStage(sideOutput = sideOutput)
  val publicStage = new GHTPublicEventStage(sideOutput = sideOutput)
  val releaseStage = new GHRecordToReleaseEventStage(sideOutput = sideOutput)
  val repoStage = new GHTRepositoryEventStage(sideOutput = sideOutput)
  val teamAddStage = new GHTTeamAddEventStage(sideOutput = sideOutput)

  val forkStage = new GHTForkEventStage(sideOutput = sideOutput)
  val issueComment = new GHTIssueCommentEventStage(sideOutput = sideOutput)
  val issueStage = new GHTIssuesEventStage(sideOutput = sideOutput)
  val memberStage = new GHTMemberEventStage(sideOutput = sideOutput)
  val prStage = new GHTPullRequestEventStage(sideOutput = sideOutput)
  val prRCStage = new GHTPullRequestReviewCommentEventStage(
    sideOutput = sideOutput)
  val watchStage = new GHTWatchEventStage(sideOutput = sideOutput)

  def main(args: Array[String]): Unit = {
    new PipelineBuilder()
      .setRestartStrategy(RestartStrategies.fixedDelayRestart(
        3,
        Time.of(10, TimeUnit.SECONDS))) // try restarting 3 times
      .enableCheckpointing(1000) // checkpointing every 1000ms
      .setBufferProperty(KafkaBuffer.AMOUNT_OF_PARTITIONS, "4")
      .setBufferProperty(KafkaBuffer.COMPRESSION_TYPE, "gzip")
      .setBufferProperty(KafkaBuffer.BROKER, "localhost:29092")
      .setBufferProperty(KafkaBuffer.ZOOKEEPER, "localhost:2181")
      .edge(
        inputStage,
        List(
          commitStage,
          commitcommentStage,
          pushStage,
          createStage,
          deleteStage,
          deploymentStage,
          deploymentStatusStage,
          gollumStage,
          memberShipStage,
          publicStage,
          releaseStage,
          repoStage,
          teamAddStage,
          forkStage,
          issueComment,
          issueStage,
          memberStage,
          prStage,
          prRCStage
        )
      )
      .edge(commitStage, new CommitStatsStage)
      .build()
      .start(args)
  }
}
