package com.example.sr

import better.files.Resource
import com.typesafe.config.Config
import io.confluent.kafka.schemaregistry.avro.{ AvroSchema, AvroSchemaProvider }
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  MockSchemaRegistryClient,
  SchemaRegistryClient
}
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.schemaregistry.{ ParsedSchema, SchemaProvider }
import wvlet.log.LogSupport

import java.net.URL
import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.Try

case class SrRestClient(schemaRegistryClient: SchemaRegistryClient) extends LogSupport {

  def getSchema(
      subjectName: String,
      lookupDeleted: Boolean = false,
      latestOnly: Boolean = false
  ): Seq[ParsedSchema] =
    schemaRegistryClient.getSchemas(subjectName, lookupDeleted, latestOnly).asScala.toSeq

  def register(
      subjectName: String,
      schemaString: String,
      schemaType: String = AvroSchema.TYPE,
      references: List[SchemaReference] = Nil
  ): Either[String, Int] = {

    val maybeSchema: Option[ParsedSchema] =
      schemaRegistryClient.parseSchema(schemaType, schemaString, references.asJava).toScala

    val res: Either[String, Int] = maybeSchema.fold(
      Left(s"failed to parse schema $schemaString").asInstanceOf[Either[String, Int]]
    ) { ps =>
      Try {
        schemaRegistryClient.register(subjectName, ps)
      }.toEither.left.map(e => e.getMessage)
    }
    res
  }

  // https://docs.confluent.io/platform/current/schema-registry/schema-deletion-guidelines.html#hard-delete-schema
  def deleteSubjects(subjects: List[String]): Map[String, Either[Throwable, util.List[Integer]]] =
    subjects.map { s =>
      s -> deleteSubject(s)
    }.toMap

  def deleteSubject(subject: String): Either[Throwable, util.List[Integer]] =
    Try(schemaRegistryClient.deleteSubject(subject, false)).flatMap { _ =>
      Try(schemaRegistryClient.deleteSubject(subject, true))
    }.toEither

  def deleteVersion(subject: String, version: String): Either[Throwable, Integer] =
    Try(schemaRegistryClient.deleteSchemaVersion(subject, version, false)).flatMap { _ =>
      Try(schemaRegistryClient.deleteSchemaVersion(subject, version, true))
    }.toEither

  def registerSchemaFromResource(
      resourcePath: String,
      subject: String,
      references: List[SchemaReference] = Nil
  ): Either[String, Int] = {
    val schemaOrError: Either[String, String] =
      Resource.asString(resourcePath).toRight(s"failed to read schema $resourcePath")
    val schemaRegistered: Either[String, Int] = schemaOrError.flatMap { schemaString =>
      register(subject, schemaString, references = references)
    }
    schemaRegistered
  }

  def setCompat(subject: String, compat: String): Either[Throwable, String] =
    Try(schemaRegistryClient.updateCompatibility(subject, compat)).toEither

}

case object SrRestClient {

  val providers: util.List[SchemaProvider] =
    List(
      new AvroSchemaProvider().asInstanceOf[SchemaProvider],
      new JsonSchemaProvider(),
      new ProtobufSchemaProvider()
    ).asJava

  val idMapCapacity = 10

  def create(configFileUrl: Option[URL] = None, configPath: Option[String] = None): SrRestClient = {
    val srRestProps = SrRestProps.create(configFileUrl, configPath)
    fromSrRestProps(srRestProps)
  }

  def fromConfig(config: Config): SrRestClient = {
    val srRestProps = SrRestProps.fromConfig(config)
    fromSrRestProps(srRestProps)
  }

  def fromSrRestProps(config: SrRestProps): SrRestClient = {

    val schemaRegistryClient: SchemaRegistryClient =
      if (config.srUrl.startsWith("mock"))
        new MockSchemaRegistryClient()
      // with scope, use this: MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE)
      else
        new CachedSchemaRegistryClient(
          List(config.srUrl).asJava,
          idMapCapacity,
          providers,
          config.srPropsMap
        )
    SrRestClient(schemaRegistryClient)
  }

}
