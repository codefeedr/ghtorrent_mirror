package org.codefeedr.experimental

import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  StateTtlConfig,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.codefeedr.experimental.GitHub.EnrichedCommit
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}
import collection.JavaConverters._

class EnrichCommitProcess
    extends CoProcessFunction[PushEvent, Commit, EnrichedCommit] {

  /** TimeToLive configuration of the (Keyed) PushEvent state */
  lazy val ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(1)) // We want PushEvents to be in State for a maximum of 1 hours.
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // We want this timer to start at the moment a PushEvent is inserted in State.
    .setStateVisibility(
      StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // If for some reason it has been expired but not cleaned, then just return it.
    .build()

  /** We need a ListState since there might be multiple PushEvents from the same repository (on which we key).  */
  private var pushEventState: ListState[PushEvent] = _

  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor =
      new ListStateDescriptor[PushEvent]("push_events", classOf[PushEvent])
    listStateDescriptor.enableTimeToLive(ttlConfig)

    pushEventState = getRuntimeContext.getListState(listStateDescriptor)
  }

  override def processElement1(
      value: PushEvent,
      ctx: CoProcessFunction[PushEvent, Commit, EnrichedCommit]#Context,
      out: Collector[EnrichedCommit]): Unit = {}

  override def processElement2(
      value: Commit,
      ctx: CoProcessFunction[PushEvent, Commit, EnrichedCommit]#Context,
      out: Collector[EnrichedCommit]): Unit = {
    val pushEventIt = pushEventState.get()

    val pushEvent = pushEventIt.asScala.find { c =>
      c.payload.commits.exists(_.sha == value.sha)
    }

  }
}
