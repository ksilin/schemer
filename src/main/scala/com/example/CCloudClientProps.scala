package com.example

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }

import java.net.URL
import java.util.Properties

case class CCloudClientProps(
    bootstrapServer: String,
    apiKey: String,
    apiSecret: String
) {

  val commonProps: Properties = new Properties()
  commonProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  commonProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
  commonProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
  commonProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")

  val saslString: String =
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${apiKey}" password="${apiSecret}";""".stripMargin
  commonProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslString)
}

case object CCloudClientProps {
  def create(configFileUrl: Option[URL] = None): CCloudClientProps = {
    val config = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    CCloudClientProps(
      config.getString("cluster.bootstrap"),
      config.getString("cluster.key"),
      config.getString("cluster.secret")
    )
  }
}
