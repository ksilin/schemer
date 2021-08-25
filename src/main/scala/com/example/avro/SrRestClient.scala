package com.example.avro

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
