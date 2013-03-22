organization := "com.github.hexx"

name := "gaeds"

version := "0.2.0"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

publishMavenStyle := true

publishArtifact in Test := false

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/hexx/gaeds</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>http://www.opensource.org/licenses/mit-license.php</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:hexx/gaeds.git</url>
    <connection>scm:git:git@github.com:hexx/gaeds.git</connection>
  </scm>
  <developers>
    <developer>
      <id>hexx</id>
      <name>Seitaro Yuuki</name>
      <url>https://github.com/hexx</url>
    </developer>
  </developers>)

libraryDependencies ++= {
  val appengineVersion = "1.7.5"
  Seq(
    "org.json4s" %% "json4s-native" % "3.1.0",
    "com.google.appengine" % "appengine-api-1.0-sdk" % appengineVersion,
    "com.google.appengine" % "appengine-api-stubs"   % appengineVersion % "test",
    "com.google.appengine" % "appengine-testing"     % appengineVersion % "test",
    "commons-codec" % "commons-codec" % "1.7",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test"
  )
}
