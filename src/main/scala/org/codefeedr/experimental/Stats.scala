package org.codefeedr.experimental
import java.util.Date

import scala.collection.mutable.Map

object Stats {

  case class Stats(key: (String, String),
                   date: String,
                   reducedCommit: ReducedCommit)

  case class ReducedCommit(user: String,
                           repo: String,
                           totalAdditions: Int,
                           totalDeletions: Int,
                           filesAdded: Int,
                           filesModified: Int,
                           filesRemoved: Int,
                           filesEdited: Map[String, Map[String, Int]])

}
