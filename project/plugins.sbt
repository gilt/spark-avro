//scalastyle:off
resolvers ++=  Seq(
  "Gilt releases" at "https://nexus.gilt.com/nexus/content/repositories/releases",
  "Third parties" at "https://nexus.gilt.com/nexus/service/local/repositories/thirdparty/content"
)

addSbtPlugin("com.gilt.sbt" % "sbt-resolvers" % "0.0.13")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
