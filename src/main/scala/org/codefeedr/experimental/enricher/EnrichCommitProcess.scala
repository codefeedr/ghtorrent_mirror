package org.codefeedr.experimental.enricher

import java.util.Date

import org.apache.flink.api.common.accumulators.{Accumulator, LongCounter}
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  StateTtlConfig
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.codefeedr.experimental.GitHub.{EnrichedCommit, Pushed}
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.{Commit, PushEvent}

import collection.JavaConverters._
import scala.compat.java8.collectionImpl.LongAccumulator

/** Minimized version of Push Event */
case class MinimizedPush(push_id: Long,
                         shaList: List[String],
                         commit_size: Int,
                         created_at: Date)

/** Low-level join which enriches Commit data with its PushEvent (if it can be found).
  *
  * @param sideOutput the output tag to send unclassified commits to.
  */
class EnrichCommitProcess()
    extends CoProcessFunction[PushEvent, Commit, EnrichedCommit] {

  /** Accumulators to keep track of (un)classified commits. */
  val unclassifiedCommits = new LongCounter()
  val classifiedCommits = new LongCounter()

  /** TimeToLive configuration of the (Keyed) PushEvent state */
  lazy val ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(1)) // We want PushEvents to be in State for a maximum of 1 hours.
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // We want this timer to start at the moment a PushEvent is inserted in State.
    .setStateVisibility(
      StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) // If for some reason it has been expired but not cleaned, then just return it.
    .cleanupFullSnapshot()
    .build()

  /** We need a ListState since there might be multiple PushEvents from the same repository (on which we key). */
  private var pushEventState: ListState[MinimizedPush] = _

  /** Opens the function by initializing the (PushEvent) list state and setting up some metrics.
    *
    * @param parameters the configuration parameters.
    */
  override def open(parameters: Configuration): Unit = {
    val listStateDescriptor =
      new ListStateDescriptor[MinimizedPush]("push_events",
                                             classOf[MinimizedPush])

    listStateDescriptor.enableTimeToLive(ttlConfig)

    pushEventState = getRuntimeContext.getListState(listStateDescriptor)
    getRuntimeContext.addAccumulator("commits_without_push",
                                     unclassifiedCommits)
    getRuntimeContext.addAccumulator("commits_with_push", classifiedCommits)
  }

  /** Stores a PushEvent in state for 1 hour.
    *
    * @param value the PushEvent to put in state as MinimizedPushEvent.
    * @param ctx the processing context.
    * @param out a collector.
    */
  override def processElement1(
      value: PushEvent,
      ctx: CoProcessFunction[PushEvent, Commit, EnrichedCommit]#Context,
      out: Collector[EnrichedCommit]): Unit =
    pushEventState.add(
      MinimizedPush(value.payload.push_id,
                    value.payload.commits.map(_.sha),
                    value.payload.size,
                    value.created_at))

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
      c.shaList.exists(_ == value.sha)
    }

    /** Get the corresponding PushEvent and collect the EnrichedCommit. */
    if (pushEventOpt.isDefined) {
      val pushEvent = pushEventOpt.get
      val pushed = Pushed(pushEvent.push_id, pushEvent.created_at)

      classifiedCommits.add(1)
      out.collect(EnrichedCommit(Some(pushed), value))
      return
    }

    /**
      * It might be possible that a commit is not embedded in a PushEvent.
      * - PushEvent > 20 commits
      * - Commit is directly pushed on GitHub
      * - No PushEvent is associated.
      */
    if (pushEventOpt.isEmpty) {

      /** If we're dealing with a push from GH, we collect it without push_id. */
      if (pushedFromGitHub(value)) {
        val pushed = Pushed(-1, value.commit.committer.date, true)

        classifiedCommits.add(1)
        out.collect(EnrichedCommit(Some(pushed), value))
        return
      }

      val pushEvents = pushEventsMoreThanTwentyCommits()

      /** If there are not PushEvents with more than 20 commits, then we can't trace it back to a PushEvent. */
      if (pushEvents.size == 0) {
        unclassifiedCommits.add(1)

        out.collect(EnrichedCommit(None, value))
        return
      }

      /** Find the PushEvent that was created after the Commit date and collect it. **/
      val pushEvent =
        pushEvents
          .find(x => value.commit.committer.date.before(x.created_at))

      /** No corresponding PushEvent can be found, then we can't trace it back to a PushEvent. */
      if (pushEvent.isEmpty) {
        unclassifiedCommits.add(1)

        out.collect(EnrichedCommit(None, value))
        return
      }

      /** Enrich Commit with closest PushEvent. */
      val pushed = Pushed(pushEvent.get.push_id, pushEvent.get.created_at)
      classifiedCommits.add(1)
      out.collect(EnrichedCommit(Some(pushed), value))
      return
    }
  }

  /** Verifies if a Commit is directly pushed/committed on the GH website.
    *
    * @param commit the commit to verify.
    * @return if a commit is pushed from GH.
    */
  def pushedFromGitHub(commit: Commit): Boolean =
    commit.commit.committer.name == "GitHub" && commit.commit.committer.email == "noreply@github.com"

  /** Returns all the PushEvents in state with more than 20 commits.
    *
    * @return The PushEvents with more than 20 commits.
    */
  def pushEventsMoreThanTwentyCommits(): List[MinimizedPush] =
    pushEventState.get().asScala.filter(_.commit_size > 20).toList
}
