package com.example

import com.typesafe.config.{ Config, ConfigFactory }
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig

import java.net.URL
import java.util
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class SrRestProps(
    srUrl: String,
    srKey: String,
    srSecret: String
) {
  val srProps: Properties = new Properties()
  srProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl)
  srProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
  srProps.put("schema.registry.basic.auth.user.info", s"$srKey:$srSecret")

  val srPropsMap: util.Map[String, String] = srProps.asInstanceOf[java.util.Map[String, String]]
}
case object SrRestProps {
  def create(configFileUrl: Option[URL] = None): SrRestProps = {
    val config: Config = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    SrRestProps(
      config.getString("sr.url"),
      config.getString("sr.key"),
      config.getString("sr.secret")
    )
  }
}
