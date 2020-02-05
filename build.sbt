organization := "com.gitlab.leafty"
version := "0.0.2-SNAPSHOT"

scalaVersion := versions.scala

name := "hudipoc"

lazy val root = project in file(".")

resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql",
    "org.apache.spark" %% "spark-avro").map(_ % versions.spark)

libraryDependencies ++= Seq(
    "org.apache.hudi" %% "hudi-spark",
    "org.apache.hudi" %% "hudi-utilities-bundle").map(_ % versions.hudi)

libraryDependencies += "mrpowers" % "spark-daria" % versions.sparkDaria

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
