organization := "com.github.hexx"

name := "gaeds"

version := "0.0.3"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= {
  val appengineVersion = "1.6.3.1"
  Seq(
    "com.google.appengine" % "appengine-api-1.0-sdk" % appengineVersion,
    "com.google.appengine" % "appengine-api-stubs"   % appengineVersion % "test",
    "com.google.appengine" % "appengine-testing"     % appengineVersion % "test",
    "org.scalatest" %% "scalatest" % "1.7.1" % "test"
  )
}
