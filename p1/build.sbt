scalaVersion := "2.13.8"

version := "0.1.0-SNAPSHOT"
name := "p1"
organization := "ch.epfl.scala"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"