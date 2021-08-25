package com.example

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import java.util.Properties

class SpecBase
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport {

  val brokerBootstrap = "pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092"
  val srUrl           = "https://psrc-lo5k9.eu-central-1.aws.confluent.cloud"

  val srKey    = "KNKURTQKVQRKKJXI"
  val srSecret = "0Bmorgabw92xfY9gaDGJ2cqe/fbSq79o1rNd+SqF9qq7U4iF7ofzOK0laaK9YrhC"

  val api_key = "GTVUUTDLNRS2VFDC"
  val secret  = "ftkrkMdns17iod1W+gu+K7FG14KP5RwMefLVgSfuhL6Wyby9D65ipX9ACLT1kXq4"
  val saslString: String =
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${secret}";""".stripMargin

  val commonProps: Properties = new Properties()
  commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap)
  commonProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslString)

  commonProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
  commonProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
  commonProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
  commonProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl)
  commonProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
  commonProps.put(SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG, s"$srKey:$srSecret")

}
