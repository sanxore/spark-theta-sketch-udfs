name := "sketches-spark"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"
val sketchesCoreVersion = "0.13.1"
val sketchesMemory = "0.12.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.yahoo.datasketches" % "sketches-core" % sketchesCoreVersion,
  "com.yahoo.datasketches" % "memory" % sketchesMemory
)

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.sonatypeRepo("public")
)
