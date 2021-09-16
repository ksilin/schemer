package com.example.sr

import java.util.Properties
import com.acme.Myrecord.MyRecord
import com.acme.Other.OtherRecord
import com.example.{ KafkaSpecHelper, SpecBase }
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  Producer,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}

import scala.jdk.CollectionConverters._

class ProtoSpec extends SpecBase {

  val protoProps: Properties = props.clone().asInstanceOf[Properties]
  protoProps.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer"
  )
  protoProps.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer"
  )
  val producer: Producer[String, MyRecord] = new KafkaProducer[String, MyRecord](protoProps)
  val consumer: Consumer[String, MyRecord] = new KafkaConsumer[String, MyRecord](protoProps)
  val otherRecord: OtherRecord             = OtherRecord.newBuilder().setOtherId(123).build()
  val myrecord: MyRecord                   = MyRecord.newBuilder().setF1("value1").setF2(otherRecord).build()

  val topic     = "testProto"
  val recordKey = "testKey"

  "must write proto to topic " in {
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topic)
    consumer.subscribe(List(topic).asJava)

    val record: ProducerRecord[String, MyRecord] =
      new ProducerRecord[String, MyRecord](topic, recordKey, myrecord)
    val sent: RecordMetadata = producer.send(record).get()
    println(sent)
    producer.close()

    val records: Iterable[ConsumerRecord[String, MyRecord]] =
      KafkaSpecHelper.fetchAndProcessRecords(consumer)
  }

}
