package com.example.sr

import com.example.avro.specificrecords.Product
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

class CustomDeserializerSpec extends SpecBase with SRBase {

  val topicName: String = suiteName
  val subjectName       = s"$topicName-value"

  val propsWithSr: Properties = props.clone().asInstanceOf[Properties]
  propsWithSr.putAll(srConfig.srPropsMap)
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
  // without it, the event type is looked up in the subject and fails
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false)

  val productProducer: Producer[String, Product] = new KafkaProducer[String, Product](propsWithSr)

  // THIS!
  propsWithSr.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    classOf[CustomProductDeserializer]
  )

  val genericProductConsumer: Consumer[String, Product] =
    new KafkaConsumer[String, Product](propsWithSr)

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

  "generic consumer with custom deserializer" in {

    genericProductConsumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, Product]] =
      KafkaSpecHelper.fetchAndProcessRecords(genericProductConsumer, process = printType)

    records.size mustBe 1

    forAll(records) { x =>
      x mustBe a[ConsumerRecord[_, _]]
      x.key mustBe a[String]
      x.value mustBe a[GenericRecord] // what sorcery is this? never fails
    }

    val productRecords: List[Product] = records.map(_.value()).toList
    productRecords.size mustBe 1
    productRecords.head mustBe product
  }

}
