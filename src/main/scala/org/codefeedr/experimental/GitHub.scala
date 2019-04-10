package org.codefeedr.experimental

import java.util.Date

import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit

object GitHub {

  case class EnrichedCommit(push_id: Option[Long],
                            push_date: Date,
                            commit: Commit)
}
