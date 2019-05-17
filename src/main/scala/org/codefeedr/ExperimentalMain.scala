package org.codefeedr

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.codefeedr.buffer.KafkaBuffer
import org.codefeedr.experimental.enricher.EnrichCommitStage
import org.codefeedr.experimental.stats.CommitsStatsStage
import org.codefeedr.experimental.stats.StatsObjects.{Stats, StatsFull}
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.plugins.elasticsearch.stages.ElasticSearchOutput
import org.codefeedr.plugins.ghtorrent.stages.GHTEventStages.GHTPushEventStage
import org.codefeedr.plugins.ghtorrent.stages.{GHTCommitStage, GHTInputStage, SideOutput}

object ExperimentalMain {
  // sideoutput configuration
  val sideOutput = SideOutput(true, sideOutputKafkaServer = "localhost:29092")

  // all stages
  val inputStage = new GHTInputStage("wzorgdrager")
  val commitStage = new GHTCommitStage()
  val pushStage = new GHTPushEventStage()
  val commitsStatsStage = new CommitsStatsStage()

  def main(args: Array[String]): Unit = {
    new PipelineBuilder()
      .setPipelineName("CF Experimental")
      .setRestartStrategy(RestartStrategies.fixedDelayRestart(
        3,
        Time.of(10, TimeUnit.SECONDS))) // try restarting 3 times
      .setStateBackend(
        new FsStateBackend("file:///home/wzorgdrager/data/flink/checkpoints"))
      .enableCheckpointing(1000) // checkpointing every 1000ms
      .setBufferProperty(KafkaBuffer.AMOUNT_OF_PARTITIONS, "4")
      .setBufferProperty(KafkaBuffer.COMPRESSION_TYPE, "gzip")
      .setBufferProperty(KafkaBuffer.BROKER, "localhost:29092")
      .setBufferProperty(KafkaBuffer.ZOOKEEPER, "localhost:2181")
      .setBufferProperty("message.max.bytes", "5000000") // max message size is 5mb
      .setBufferProperty("max.request.size", "5000000") // max message size is 5 mb
      //.setBufferProperty("auto.offset.reset", "latest")
      .edge(inputStage, List(commitStage, pushStage))
      .edge(List(pushStage, commitStage), new EnrichCommitStage())
      .edge(commitStage, commitsStatsStage)
      .edge(commitsStatsStage, new ElasticSearchOutput[StatsFull]("commit_stats", "es_commit_stats"))
      .build()
      .start(args)
  }
}
