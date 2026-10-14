name := "github-analytics-spark"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion   = "3.5.0"
val icebergVersion = "1.4.3"

// Source directories: ETL batch jobs + GraphX analytics only.
// Streaming (Kafka) and ML (MLlib retention prediction) are excluded for now.
Compile / unmanagedSourceDirectories ++= Seq(
  baseDirectory.value / "etl/src/main/scala",
  baseDirectory.value / "graph/src/main/scala",
)

libraryDependencies ++= Seq(
  // Spark core (provided by the cluster — NOT bundled in the jar)
  "org.apache.spark" %% "spark-core"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"    % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",

  // Apache Iceberg Spark runtime (bundled: NOT in the Spark image)
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,

  // PostgreSQL JDBC driver (bundled: for optional JDBC sink in GraphAnalytics)
  "org.postgresql" % "postgresql" % "42.7.2",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
)

// Assembly jar merge strategy — Spark classes are excluded below
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// Exclude Spark itself from the fat jar (it's provided on the cluster)
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { f =>
    val n = f.data.getName
    n.startsWith("spark-") || n.startsWith("scala-library")
  }
}

Test / fork := true
