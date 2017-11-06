import sbt.Keys.libraryDependencies

name := "KafkaTwitterDockerExample"

version := "1.0"

scalaVersion := "2.11.8"

def dockerSettings(debugPort: Option[Int] = None) = Seq(

  dockerfile in docker := {
    val artifactSource: File = assembly.value
    val artifactTargetPath = s"/project/${artifactSource.name}"
    val scriptSourceDir = baseDirectory.value / "../scripts"
    val projectDir = "/project/"

    new Dockerfile {
      from("saumitras01/java:1.8.0_111")
      add(artifactSource, artifactTargetPath)
      copy(scriptSourceDir, projectDir)
      entryPoint(s"/project/start.sh")
      cmd(projectDir, s"${name.value}", s"${version.value}")
    }
  },
  imageNames in docker := Seq(
    ImageName(s"saumitras01/${name.value}:latest")
  )
)

lazy val producer = (project in file("tweet-producer"))
  .enablePlugins(sbtdocker.DockerPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.twitter4j" % "twitter4j-core" % "4.0.4",
      "org.twitter4j" % "twitter4j-stream" % "4.0.4",
      "org.apache.kafka" % "kafka_2.11" % "0.10.0.0" withSources() exclude("org.slf4j","slf4j-log4j12") exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
    ),
    dockerSettings()
  )

lazy val consumer = (project in file("tweet-consumer"))
  .enablePlugins(sbtdocker.DockerPlugin)
  .settings(
    libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.0.0" withSources() exclude("org.slf4j","slf4j-log4j12") exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri"),
    dockerSettings()
  )

