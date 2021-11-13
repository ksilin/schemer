package com.example.avro

import com.example.avro.enm.Enums.{ Suit3, Suit4 }
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer

package object enm {

  case class Four(Suit: Suit4)
  case class Three(Suit: Suit3)

  class TestSerializer(client: SchemaRegistryClient) extends KafkaAvroSerializer(client) {

    //override def autoRegisterSchema: Boolean = true
    autoRegisterSchema = true

    // super may not be used on variable autoRegisterSchema; super can only be used to select a member that is a method or type
    // super.autoRegisterSchema = true

    def serialize(subject: String, obj: Object, schema: AvroSchema): Array[Byte] =
      serializeImpl(subject, obj, schema)
  }

}
