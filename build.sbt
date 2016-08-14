name := "idark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "com.jcraft" % "jsch" % "0.1.53",
  "com.amazonaws" % "aws-java-sdk" % "1.11.26"
)