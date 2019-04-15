package org.codefeedr.experimental

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.codefeedr.experimental.StatsObjects.Stats
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

class CommitsStatsProcess extends ProcessFunction[Commit, Stats] {

  override def open(parameters: Configuration): Unit = {}

  override def processElement(value: Commit,
                              ctx: ProcessFunction[Commit, Stats]#Context,
                              out: Collector[Stats]): Unit = ???
}
