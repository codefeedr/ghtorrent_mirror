package org.codefeedr.experimental.stats

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.codefeedr.experimental.stats.StatsObjects.Stats

class InsertAndReplaceMongoSink extends RichSinkFunction[Stats] {

  override def open(parameters: Configuration): Unit = {}

  override def invoke(value: Stats, context: SinkFunction.Context[_]): Unit = {}
}
