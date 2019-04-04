package org.codefeedr.experimental
import scala.collection.mutable.Map

object Stats {

  case class Stats()

  case class ReducedCommit(user: String,
                           repo: String,
                           totalAdditions: Int,
                           totalDeletions: Int,
                           filesAdded: Int,
                           filesModified: Int,
                           filesRemoved: Int,
                           filesEdited: Map[String, Map[String, Int]])

}
