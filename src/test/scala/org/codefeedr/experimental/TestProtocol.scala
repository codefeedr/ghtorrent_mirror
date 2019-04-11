package org.codefeedr.experimental

import java.util.Date

import org.codefeedr.plugins.ghtorrent.protocol.GHTorrent._id
import org.codefeedr.plugins.ghtorrent.protocol.GitHub._

object TestProtocol {

  val commitUser = CommitUser("wouter", "wouter", new Date())

  val verificationObject = Verification(
    true,
    "valid",
    Some(
      "-----BEGIN PGP SIGNATURE-----\\n\\nwsBcBAABCAAQBQJcrZL/CRBK7hj4Ov3rIwAAdHIIAIi9LScaIcNDuMR1NDlKFSpw\\n7XalLld6y+lwhqEr8C+XJod+zl3xQjv6lYH65MVesPIdgSNV780K8cKqcOLJ9YV3\\nQy7vfTQE+CAIymR/3M4bkkJ6+SJ7+WRhl6o3Y6SNpKZjtKQy6sQRUkFASL3n8Y4W\\nMXlXUtASlQ7anaA5AsUWW7gLQsnH3Fi+WqOc9vFWSF++SyEEQg16Z9tF+26snp4K\\ntU+q6gSoXIMBJwMAjmONANkUVhp/bNeqxOZbGSJgRC1rWDFaRaKpMqe+yTai6Zla\\ngfbMSYY7D5exscb4IY1+m4kf9NPWD4fUxQGttAbJMRPq8fFPykA5Ri14pb13o+A=\\n=J+ms\\n-----END PGP SIGNATURE-----\\n"),
    Some(
      "tree ab5d050c88230206cfd1d595ffd1464dff751fc2\\nauthor kelvin-otieno <kelvinotieno06@gmail.com> 1554879231 +0300\\ncommitter GitHub <noreply@github.com> 1554879231 +0300\\n\\nInitial commit")
  )

  val unverifiedObject = Verification(false, "", None, None)

  val verifiedCommit =
    Commit(
      _id(""),
      "",
      "",
      "",
      CommitData(commitUser, commitUser, "", Tree(""), 0, verificationObject),
      None,
      None,
      List(),
      None,
      List())

  val unverifiedCommit =
    Commit(
      _id(""),
      "",
      "",
      "",
      CommitData(commitUser, commitUser, "", Tree(""), 0, unverifiedObject),
      None,
      None,
      List(),
      None,
      List())

}
