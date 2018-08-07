organization := "com.gitlab.leafty"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.11.12"
name := "test-hudi"

lazy val root = project in file(".")

resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "com.uber.hoodie" % "hoodie-spark" % "0.4.2"

libraryDependencies += "mrpowers" % "spark-daria" % "2.3.0_0.18.0"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0"
//libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
