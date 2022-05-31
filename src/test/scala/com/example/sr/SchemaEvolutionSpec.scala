package com.example.sr

import com.example.{ KafkaSpecHelper, SRBase, SpecBase }
import com.example.avro.specificrecords.{ Product, ProductV2 }
import io.confluent.kafka.serializers.{
  AbstractKafkaSchemaSerDeConfig,
  KafkaAvroDeserializerConfig
}
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerRecord }

import java.util.Properties
import scala.jdk.CollectionConverters._

class SchemaEvolutionSpec extends SpecBase with SRBase {

  val topicName: String = suiteName
  val subjectName       = s"$topicName-value"

  val propsWithSr: Properties = props.clone().asInstanceOf[Properties]
  propsWithSr.putAll(srConfig.srPropsMap)
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
  // without it, the event type is looked up in the subject and fails
  propsWithSr.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false)
  // otherwise, Deserializer will only work with GenericRecords and fail once you try to extract the actual clas you want
  propsWithSr.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)

  val productProducer: Producer[String, Product] = new KafkaProducer[String, Product](propsWithSr)
  val productV2Producer: Producer[String, ProductV2] =
    new KafkaProducer[String, ProductV2](propsWithSr)

  val productConsumer: Consumer[String, Product] = new KafkaConsumer[String, Product](propsWithSr)

  // second consumer needs to restart from earliest or msgs need to be reproduced
  propsWithSr.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group-2"
  )
  val productV2Consumer: Consumer[String, ProductV2] =
    new KafkaConsumer[String, ProductV2](propsWithSr)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 10.99)
  val productV2: ProductV2 = ProductV2(
    product_id = 1,
    product_name = "myProductV2",
    product_price = 11.99,
    product_description = "v2 description"
  )

  srClient.deleteSubject(subjectName)

  KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

  val productProducerRecord = new ProducerRecord[String, Product](topicName, "prodKey", product)
  productProducer.send(productProducerRecord).get()

  // TODO - clumsy, change it
  // subject has been deleted, so can only set compat after new schema has revived the subject
  srClient.setCompat(subjectName, "FORWARD")

  val productV2ProducerRecord =
    new ProducerRecord[String, ProductV2](topicName, "prodKeyV2", productV2)
  productV2Producer.send(productV2ProducerRecord).get()

  "forward compat lets OLD CONSUMER read old records and new records with added field" in {

    productConsumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, Product]] =
      KafkaSpecHelper.fetchAndProcessRecords(productConsumer)

    val products: List[Product] = records.map(_.value()).toList
    products foreach println
    records.size mustBe 2
  }

  "forward compat makes NEW CONSUMER break on old records without added field" in {

    productV2Consumer.subscribe(List(topicName).asJava)

    val records: Iterable[ConsumerRecord[String, ProductV2]] =
      KafkaSpecHelper.fetchAndProcessRecords(productV2Consumer)

    //java.lang.ClassCastException: class com.examples.schema.Product cannot be cast to class com.examples.schema.ProductV2
    // (com.examples.schema.Product and com.examples.schema.ProductV2 are in unnamed module of loader 'app')
    val ex = intercept[ClassCastException](records.map(_.value()).toList)
    println(ex)
    ex.getMessage.contains(
      "Product cannot be cast to class com.example.avro.specificrecords.ProductV2"
    ) mustBe true

  }

}
