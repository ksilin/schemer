package com.example.avro

import better.files.Resource
import io.circe
import io.confluent.kafka.schemaregistry.{ AbstractSchemaProvider, ParsedSchema, SchemaProvider }
import io.confluent.kafka.schemaregistry.avro.{ AvroSchema, AvroSchemaProvider }
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaRegistryClient,
  SchemaRegistryClientConfig
}
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import wvlet.log.LogSupport

import java.util
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional
import scala.util.Try

case class SrRestConfig(url: String, credentials: String) {

  val asMap = Map(
    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url,
    SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE  -> "USER_INFO",
    SchemaRegistryClientConfig.USER_INFO_CONFIG               -> credentials
  )
}

case class SrRestClient(config: SrRestConfig) extends LogSupport {
  import com.example.avro.SrRestClient._

  val schemaRegistryClient: SchemaRegistryClient =
    new CachedSchemaRegistryClient(
      List(config.url).asJava,
      idMapCapacity,
      providers,
      config.asMap.asJava
    )

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

  // schemaRegistryClient.register(subjectName, parsedSchema)
}

case object SrRestClient {

  val providers: util.List[SchemaProvider] =
    List(
      new AvroSchemaProvider().asInstanceOf[SchemaProvider],
      new JsonSchemaProvider(),
      new ProtobufSchemaProvider()
    ).asJava

  val idMapCapacity = 10

}
