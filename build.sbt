name := "chinmay_nautiyal_hw2"

version := "0.1"

scalaVersion := "2.13.1"


libraryDependencies ++= Seq("com.typesafe" % "config" % "1.3.4",
"ch.qos.logback" % "logback-classic" % "1.2.3",
 "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
 "org.scalactic" %% "scalactic" % "3.0.8",
 "org.scalatest" %% "scalatest" % "3.0.8" % "test"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
