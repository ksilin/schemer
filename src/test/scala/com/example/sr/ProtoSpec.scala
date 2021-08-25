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

  val props: Properties = config.commonProps.clone().asInstanceOf[Properties]
  props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer"
  )

  props.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  props.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer"
  )

  props.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group"
  )

  props.put(
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
    "earliest"
  )

  val producer: Producer[String, MyRecord] = new KafkaProducer[String, MyRecord](props)
  val consumer: Consumer[String, MyRecord] = new KafkaConsumer[String, MyRecord](props)
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
