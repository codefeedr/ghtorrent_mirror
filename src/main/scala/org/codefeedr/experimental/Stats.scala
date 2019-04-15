package org.codefeedr.experimental
import java.util.Date

import scala.collection.mutable.Map

object StatsObjects {

  case class Stats(date: String, commitStats: CommitStats)

  case class CommitStats(totalCommits: Long,
                         totalAdditions: Long,
                         totalDeletions: Long,
                         filesAdded: Long,
                         filesModified: Long,
                         filesRemoved: Long,
                         filesEdited: List[Extension])

  case class Extension(name: String,
                       additions: Long,
                       deletions: Long,
                       created: Long)

}
