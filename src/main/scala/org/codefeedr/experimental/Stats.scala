package org.codefeedr.experimental
import java.util.Date

import scala.collection.mutable.Map

object StatsObjects {

  case class Stats(date: String, reducedCommit: ReducedCommits)

  case class ReducedCommits(user: String,
                            repo: String,
                            totalCommits: Int,
                            totalAdditions: Int,
                            totalDeletions: Int,
                            filesAdded: Int,
                            filesModified: Int,
                            filesRemoved: Int,
                            filesEdited: Map[String, Map[String, Int]])

}
