package com.example

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
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

  val cloudProps: CCloudClientProps = CCloudClientProps.create()

  val adminClient: AdminClient = AdminClient.create(cloudProps.commonProps)

  val props: Properties = cloudProps.commonProps.clone().asInstanceOf[Properties]

  props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false)
  // without it, the event type is looked up in the subject and fails
  props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true)

  props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroSerializer"
  )

  //  props.put(
  //    AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID,
  //    "io.confluent.kafka.serializers.KafkaAvroSerializer"
  //  )

  props.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroDeserializer"
  )

  props.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group"
  )

  props.put(
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
    "earliest"
  )
}
