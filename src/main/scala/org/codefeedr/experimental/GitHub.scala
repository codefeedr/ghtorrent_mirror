package org.codefeedr.experimental

import java.util.Date

import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

object GitHub {

  case class Pushed(push_id: Long,
                    push_date: Date,
                    pushed_from_github: Boolean = false)

  case class EnrichedCommit(push: Option[Pushed], commit: Commit)
}
