package org.codefeedr.experimental
import java.util.Date

import scala.collection.mutable.Map

object StatsObjects {

  case class Stats(date: String, reducedCommit: ReducedCommits)

  case class ReducedCommits(totalCommits: Long,
                            totalAdditions: Long,
                            totalDeletions: Long,
                            filesAdded: Long,
                            filesModified: Long,
                            filesRemoved: Long,
                            filesEdited: Map[String, Map[String, Long]])

}
