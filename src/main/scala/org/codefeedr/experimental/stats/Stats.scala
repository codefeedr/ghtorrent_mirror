package org.codefeedr.experimental.stats

import java.util.Date

object StatsObjects {

  case class Stats(date: String, commitStats: CommitStats)

  case class StatsFull(date: Date, commitStats: CommitStats)

  case class CommitStats(totalCommits: Long, filesEdited: List[Extension])

  case class Extension(name: String,
                       additions: Long,
                       deletions: Long,
                       added: Long,
                       removed: Long,
                       modified: Long)

}
