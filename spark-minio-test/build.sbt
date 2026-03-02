name := "spark-minio-test"
version := "1.0.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"       % sparkVersion % "provided",
  // Hadoop AWS (S3A - MinIO compatible)
  "org.apache.hadoop" % "hadoop-aws"      % "3.3.4"      % "provided",
  "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262" % "provided"
)

// fat jar 빌드 설정
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Spark provided 의존성 제외
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter { f =>
    val name = f.data.getName
    name.startsWith("spark-") || name.startsWith("hadoop-") || name.startsWith("scala-library")
  }
}
