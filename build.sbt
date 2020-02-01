organization := "com.gitlab.leafty"
version := "0.0.2-SNAPSHOT"

scalaVersion := versions.scala

name := "hudipoc"

lazy val root = project in file(".")

resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % versions.spark
libraryDependencies += "org.apache.spark" %% "spark-avro" % versions.spark

libraryDependencies += "org.apache.hudi" %% "hudi-spark" % versions.hudi
libraryDependencies += "org.apache.hudi" %% "hudi-utilities-bundle" % versions.hudi

libraryDependencies += "mrpowers" % "spark-daria" % "2.3.0_0.18.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
