package com.example.sr

import com.example.avro.specificrecords.{
  Product,
  ProductWithDescription,
  ProductWithOptionalDescription
}
import com.example.util.{ KafkaSpecHelper, SRBase, SpecBase }
import io.confluent.kafka.serializers.{
  AbstractKafkaSchemaSerDeConfig,
  KafkaAvroDeserializerConfig
}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerRecord }
import org.scalatest.Inspectors.forAll

import java.util.Properties
import scala.jdk.CollectionConverters._

class GenericSpecificSpec extends SpecBase with SRBase {

  val topicName: String = suiteName
  val subjectName       = s"$topicName-value"

  val propsWithSr: Properties = props.clone().asInstanceOf[Properties]
  propsWithSr.putAll(srConfig.srPropsMap)
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
  // without it, the event type is looked up in the subject and fails
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false)

  val productProducer: Producer[String, Product] = new KafkaProducer[String, Product](propsWithSr)

  // this is the cloud here - either tries to use a specific record or not
  propsWithSr.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false)
  val genericProductConsumer: Consumer[String, Product] =
    new KafkaConsumer[String, Product](propsWithSr)
  propsWithSr.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group-2"
  )
  val genericRecordConsumer: Consumer[String, GenericRecord] =
    new KafkaConsumer[String, GenericRecord](propsWithSr)
  propsWithSr.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
  // second consumer needs to restart from earliest or msgs need to be reproduced
  propsWithSr.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group-3"
  )
  val specificConsumer: Consumer[String, Product] = new KafkaConsumer[String, Product](propsWithSr)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 10.99)

  def printType[K, V](x: ConsumerRecord[K, V]): Unit = {
    println(s"class of record: ${x.getClass}")
    println(s"class of key: ${x.key().getClass}")
    println(s"class of value: ${x.value().getClass}")
  }

  srClient.deleteSubject(subjectName)
  KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

  val productProducerRecord = new ProducerRecord[String, Product](topicName, "prodKey", product)
  productProducer.send(productProducerRecord).get()

  "specific consumer" in {

    specificConsumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, Product]] =
      KafkaSpecHelper.fetchAndProcessRecords(specificConsumer)

    records.size mustBe 1
    forAll(records) { x =>
      x mustBe a[ConsumerRecord[_, _]]
      x.key mustBe a[String]
      x.value mustBe a[Product]
    }
    // mapping to product works
    val products: List[Product] = records.map(_.value()).toList
  }

  "generic consumer" in {

    genericProductConsumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, Product]] =
      KafkaSpecHelper.fetchAndProcessRecords(genericProductConsumer, process = printType)

    records.size mustBe 1

    forAll(records) { x =>
      x mustBe a[ConsumerRecord[_, _]]
      x.key mustBe a[String]
      x.value mustBe a[GenericRecord]
    }
    // mapping to Product breaks
    val ex = intercept[ClassCastException](records.map(_.value()).toList)
    println(ex)
  }

  "generic consumer properly specified" in {

    genericRecordConsumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, GenericRecord]] =
      KafkaSpecHelper.fetchAndProcessRecords(genericRecordConsumer, process = printType)

    records.size mustBe 1

    forAll(records) { x =>
      x mustBe a[ConsumerRecord[_, _]]
      x.key mustBe a[String]
      x.value mustBe a[GenericRecord]
    }

    // mapping to Product breaks
    val productRecords: List[GenericRecord] = records.map(_.value()).toList
    productRecords.size mustBe 1
    productRecords foreach println

  }

}
