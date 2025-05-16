ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_FP_Discount_Rule_Engine",
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "42.7.2"
    )
  )
