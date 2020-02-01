organization := "com.gitlab.leafty"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.12.10"
name := "hudipoc"

lazy val root = project in file(".")

resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"

//libraryDependencies += "org.apache.parquet" %% "parquet-avro" % "1.10.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.4"

libraryDependencies += "org.apache.hudi" %% "hudi-spark" % "0.5.1-incubating"
libraryDependencies += "org.apache.hudi" %% "hudi-utilities-bundle" % "0.5.1-incubating"


libraryDependencies += "mrpowers" % "spark-daria" % "2.3.0_0.18.0"

//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.3.6"
//libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
