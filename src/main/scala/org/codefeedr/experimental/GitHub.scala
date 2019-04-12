package org.codefeedr.experimental

import java.util.Date

import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

object GitHub {

  case class Pushed(push_id: Option[Long], push_date: Date)
  case class EnrichedCommit(pushed: Option[Pushed], commit: Commit)
}
