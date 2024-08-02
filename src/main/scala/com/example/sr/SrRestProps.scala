package com.example.sr

import com.typesafe.config.{ Config, ConfigFactory }
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig

import java.net.URL
import java.util
import java.util.Properties

case class SrRestProps(
    srUrl: String,
    srKey: Option[String] = None,
    srSecret: Option[String] = None
) {
  val srProps: Properties = new Properties()
  srProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl)
  srKey.zip(srSecret).map {
    case (key, secret) =>
      srProps.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
      srProps.put("schema.registry.basic.auth.user.info", s"$key:$secret")
  }

  val srPropsMap: util.Map[String, String] = srProps.asInstanceOf[java.util.Map[String, String]]
}

case object SrRestProps {
  def create(configFileUrl: Option[URL] = None, configPath: Option[String] = None): SrRestProps = {
    val topLevelConfig = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    val config         = configPath.map(path => topLevelConfig.getConfig(path)).getOrElse(topLevelConfig)
    fromConfig(config)
  }

  def fromConfig(config: Config): SrRestProps =
    SrRestProps(
      config.getString("sr.url"),
      if (config.hasPath("sr.key"))
        Some(config.getString("sr.key"))
      else None,
      if (config.hasPath("sr.secret"))
        Some(config.getString("sr.secret"))
      else None
    )
}
