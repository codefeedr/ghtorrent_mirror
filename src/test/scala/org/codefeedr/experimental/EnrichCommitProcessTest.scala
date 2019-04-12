package org.codefeedr.experimental

import org.scalatest.FunSuite
import org.codefeedr.experimental.TestProtocol._

class EnrichCommitProcessTest extends FunSuite {

  test("GitHub commit should be correctly verified.") {
    val enrichCommitProcess = new EnrichCommitProcess()
    assert(enrichCommitProcess.pushedFromGitHub(verifiedCommit))
  }

  test("GitHub commit could also not be verified.") {
    val enrichCommitProcess = new EnrichCommitProcess()

    assert(!enrichCommitProcess.pushedFromGitHub(unverifiedCommit))
  }
}
