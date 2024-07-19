ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12" // Ensure compatibility with Spark 2.4.7

lazy val root = (project in file("."))
  .settings(
    name := "E-Commerce Data Analysis",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.7",
      "org.apache.spark" %% "spark-sql" % "2.4.7"
    ),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
  )
