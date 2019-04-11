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
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.codefeedr.experimental.GitHub.EnrichedCommit
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}

import collection.JavaConverters._

case class UnclassifiedCommit(commit: Commit,
                              pushEvents: List[PushEvent],
                              reason: String)

class EnrichCommitProcess(sideOutput: OutputTag[UnclassifiedCommit])
    extends CoProcessFunction[PushEvent, Commit, EnrichedCommit] {

  /** TimeToLive configuration of the (Keyed) PushEvent state */
  lazy val ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(1)) // We want PushEvents to be in State for a maximum of 1 hours.
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // We want this timer to start at the moment a PushEvent is inserted in State.
    .setStateVisibility(
      StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // If for some reason it has been expired but not cleaned, then just return it.
    .build()

  /** We need a ListState since there might be multiple PushEvents from the same repository (on which we key). */
  private var pushEventState: ListState[PushEvent] = _

  /** Opens the function by initializing the (PushEvent) list state.
    *
    * @param parameters the configuration parameters.
    */
  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor =
      new ListStateDescriptor[PushEvent]("push_events", classOf[PushEvent])
    listStateDescriptor.enableTimeToLive(ttlConfig)

    pushEventState = getRuntimeContext.getListState(listStateDescriptor)
  }

  /** Stores a PushEvent in state for 1 hour.
    *
    * @param value the PushEvent to put in state.
    * @param ctx the processing context.
    * @param out a collector.
    */
  override def processElement1(
      value: PushEvent,
      ctx: CoProcessFunction[PushEvent, Commit, EnrichedCommit]#Context,
      out: Collector[EnrichedCommit]): Unit = pushEventState.add(value)

  /** Processes and enriches a Commit based on the PushEvents in state.
    *
    * @param value the commit to enrich.
    * @param ctx the processing context.
    * @param out a collector.
    */
  override def processElement2(
      value: Commit,
      ctx: CoProcessFunction[PushEvent, Commit, EnrichedCommit]#Context,
      out: Collector[EnrichedCommit]): Unit = {
    val pushEventIt = pushEventState.get()

    /** We find the PushEvent with the matching SHA. */
    val pushEventOpt = pushEventIt.asScala.find { c =>
      c.payload.commits.exists(_.sha == value.sha)
    }

    /** Get the corresponding PushEvent and collect the EnrichedCommit. */
    if (pushEventOpt.isDefined) {
      val pushEvent = pushEventOpt.get
      out.collect(
        EnrichedCommit(Some(pushEvent.payload.push_id),
                       pushEvent.created_at,
                       value))
    }

    /**
      * It might be possible that a commit is not embedded in a PushEvent.
      * - PushEvent > 20 commits
      * - Commit is directly pushed on GitHub
      */
    if (pushEventOpt.isEmpty) {

      /** If we're dealing with a push from GH, we collect it without push_id. */
      if (pushedFromGitHub(value)) {
        out.collect(EnrichedCommit(None, value.commit.committer.date, value))

        return
      }

      val pushEvents = pushEventsMoreThanTwentyCommits()

      /** If there are not PushEvents with more than 20 commits, then something is going wrong. */
      if (pushEvents.size == 0) {
        ctx.output(
          sideOutput,
          UnclassifiedCommit(value,
                             pushEventState.get().asScala.toList,
                             "No PushEvents with more than 20 commits."))

        return
      }

      /** Find the PushEvent that was created after the Commit date and collect it. **/
      val pushEvent =
        pushEvents
          .find(x => value.commit.committer.date.before(x.created_at))

      /** No corresponding PushEvent can be found, something is going wrong. */
      if (pushEvent.isEmpty) {
        ctx.output(
          sideOutput,
          UnclassifiedCommit(value,
                             pushEventState.get().asScala.toList,
                             "No PushEvents with more than 20 commits."))

        return
      }

      /** Enrich Commit with closest PushEvent. */
      out.collect(
        EnrichedCommit(Some(pushEvent.get.payload.push_id),
                       pushEvent.get.created_at,
                       value))
    }
  }

  /** Verifies if a Commit is directly pushed/committed on the GH website.
    *
    * @param commit the commit to verify.
    * @return if a commit is pushed from GH.
    */
  def pushedFromGitHub(commit: Commit): Boolean =
    commit.commit.verification.payload match {

      /** If the payload contains GitHub as committer it is directly committed from GH */
      case Some(reason) if reason.contains("committer GitHub") => true
      case _                                                   => false
    }

  /** Returns all the PushEvents in state with more than 20 commits.
    *
    * @return The PushEvents with more than 20 commits.
    */
  def pushEventsMoreThanTwentyCommits(): List[PushEvent] =
    pushEventState.get().asScala.filter(_.payload.commits.size > 20).toList
}
