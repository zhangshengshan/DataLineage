ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "SqlFieldLinage",
    idePackagePrefix := Some("io.github.zhangshengshan")
  )

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

libraryDependencies ++= Seq(
  "org.apache.calcite" % "calcite-core" % "1.37.0",
  "org.apache.calcite" % "calcite-linq4j" % "1.37.0",
  "mysql" % "mysql-connector-java" % "8.0.33"
)