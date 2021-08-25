package com.example.sr

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }

import java.net.URL
import java.util.Properties

case class CloudProps(
    bootstrapBroker: String,
    apiKey: String,
    apiSecret: String,
    srUrl: String,
    srKey: String,
    srSecret: String
) extends ClientProps {

  val commonProps: Properties = new Properties()
  commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker)
  commonProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
  commonProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
  commonProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
  commonProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl)
  commonProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
  commonProps.put("schema.registry.basic.auth.user.info", s"$srKey:$srSecret")

  val saslString: String =
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${apiKey}" password="${apiSecret}";""".stripMargin
  commonProps.setProperty("sasl.jaas.config", saslString)
}

case object CloudProps {
  def create(configFileUrl: Option[URL] = None): CloudProps = {
    val config = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    CloudProps(
      config.getString("cluster.bootstrap"),
      config.getString("cluster.key"),
      config.getString("cluster.secret"),
      config.getString("sr.url"),
      config.getString("sr.key"),
      config.getString("sr.secret")
    )
  }
}
