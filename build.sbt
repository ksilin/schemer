import sbtprotoc.ProtocPlugin.autoImport.PB
// *****************************************************************************
// Projects
// *****************************************************************************

lazy val schemer =
  project
    .in(file("."))
    .settings(commonSettings)
    .settings(
      libraryDependencies ++= Seq(
          library.clients,
          library.kafka,
          library.sttp,
          library.sttpBackendOkHttp,
          library.sttpCirce,
          library.circeGeneric,
          library.circeParser,
          library.avro4s,
          library.avro4sKafka,
          library.srClient,
          library.jsonSchemaProvider,
          library.protoSchemaProvider,
          library.protobuf,
          library.protobufSerializers,
          library.avroSerializers,
          library.scalaPB,
          library.betterFiles,
          library.config,
          library.airframeLog,
          library.logback,
          library.scalatest % Test
        ),
      libraryDependencies ~= {
        _.map(_.exclude("org.slf4j", "slf4j-log4j12"))
      }
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val kafka       = "2.8.0"
      val cp          = "6.2.0"
      val circe       = "0.13.0"
      val avro4s      = "4.0.4"
      val sttp        = "3.3.13"
      val betterFiles = "3.9.1"
      val config = "1.4.1"
      val airframeLog = "20.12.1"
      val scalatest   = "3.2.0"
      val logback     = "1.2.3"
    }
    val clients             = "org.apache.kafka"               % "kafka-clients"                % Version.kafka
    val kafka               = "org.apache.kafka"              %% "kafka"                        % Version.kafka
    val protobufSerializers = "io.confluent"                   % "kafka-protobuf-serializer"    % Version.cp
    val avroSerializers     = "io.confluent"                   % "kafka-avro-serializer"        % Version.cp
    val srClient            = "io.confluent"                   % "kafka-schema-registry-client" % Version.cp
    val jsonSchemaProvider  = "io.confluent"                   % "kafka-json-schema-provider"   % Version.cp
    val protoSchemaProvider = "io.confluent"                   % "kafka-protobuf-provider"      % Version.cp
    val protobuf            = "com.google.protobuf"            % "protobuf-java"                % "3.12.2"
    val sttp                = "com.softwaremill.sttp.client3" %% "core"                         % Version.sttp
    val sttpBackendOkHttp   = "com.softwaremill.sttp.client3" %% "okhttp-backend"               % Version.sttp
    val sttpCirce           = "com.softwaremill.sttp.client3" %% "circe"                        % Version.sttp
    val circeGeneric        = "io.circe"                      %% "circe-generic"                % Version.circe
    val circeParser         = "io.circe"                      %% "circe-parser"                 % Version.circe
    val avro4s              = "com.sksamuel.avro4s"           %% "avro4s-core"                  % Version.avro4s
    val avro4sKafka         = "com.sksamuel.avro4s"           %% "avro4s-kafka"                 % Version.avro4s
    val scalaPB             = "com.thesamet.scalapb"          %% "compilerplugin"               % "0.10.8"
    val betterFiles = "com.github.pathikrit" %% "better-files" % Version.betterFiles
    val config = "com.typesafe" % "config" % Version.config
    val airframeLog         = "org.wvlet.airframe"            %% "airframe-log"                 % Version.airframeLog
    val logback             = "ch.qos.logback"                 % "logback-classic"              % Version.logback
    val scalatest           = "org.scalatest"                 %% "scalatest"                    % Version.scalatest
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.3",
    organization := "default",
    organizationName := "konstantin.silin",
    startYear := Some(2020),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalacOptions ++= Seq(
        "-unchecked",
        "-deprecation",
        "-language:_",
        "-encoding",
        "UTF-8",
        "-Ywarn-unused:imports"
      ),
    // testFrameworks += new TestFramework("munit.Framework"),
    resolvers ++= Seq(
        "confluent" at "https://packages.confluent.io/maven/",
        "jitpack" at "https://jitpack.io"
      ),
    scalafmtOnCompile := true
  )

Compile / PB.targets := Seq(
  // scalapb.gen() -> (sourceManaged in Compile).value / "scalapb",
  PB.gens.java                        -> (Compile / sourceManaged).value,
  scalapb.gen(javaConversions = true) -> (Compile / sourceManaged).value
)

Compile / avroSourceDirectories += (Compile / resourceDirectory).value / "avro"
Compile / avroSpecificSourceDirectories += (Compile / resourceDirectory).value / "avro"
Compile / sourceGenerators += (Compile / avroScalaGenerate).taskValue
