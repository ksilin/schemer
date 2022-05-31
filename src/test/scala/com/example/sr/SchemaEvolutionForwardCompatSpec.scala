package com.example.sr

import com.example.{ KafkaSpecHelper, SRBase, SpecBase }
import com.example.avro.specificrecords.{
  Product,
  ProductWithDescription,
  ProductWithOptionalDescription
}
import io.confluent.kafka.serializers.{
  AbstractKafkaSchemaSerDeConfig,
  KafkaAvroDeserializerConfig
}
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerRecord }

import java.util.Properties
import scala.jdk.CollectionConverters._

class SchemaEvolutionForwardCompatSpec extends SpecBase with SRBase {

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
  val productWithDescriptionProducer: Producer[String, ProductWithDescription] =
    new KafkaProducer[String, ProductWithDescription](propsWithSr)
  val productWithOptDescriptionProducer: Producer[String, ProductWithOptionalDescription] =
    new KafkaProducer[String, ProductWithOptionalDescription](propsWithSr)

  val productConsumer: Consumer[String, Product] = new KafkaConsumer[String, Product](propsWithSr)

  // second consumer needs to restart from earliest or msgs need to be reproduced
  propsWithSr.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group-2"
  )
  val productWithDescriptionConsumer: Consumer[String, ProductWithDescription] =
    new KafkaConsumer[String, ProductWithDescription](propsWithSr)

  propsWithSr.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    s"$suiteName-group-3"
  )
  val productWithOptDescriptionConsumer: Consumer[String, ProductWithOptionalDescription] =
    new KafkaConsumer[String, ProductWithOptionalDescription](propsWithSr)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 10.99)
  val productWithDescription: ProductWithDescription = ProductWithDescription(
    product_id = 1,
    product_name = "myProductV2",
    product_price = 11.99,
    product_description = "v2 description"
  )
  val productWithOptionalDescriptionPresent: ProductWithOptionalDescription =
    ProductWithOptionalDescription(
      product_id = 1,
      product_name = "myProductV3",
      product_price = 12.99,
      product_description = Some("description")
    )

  val productWithOptionalDescriptionAbsent: ProductWithOptionalDescription =
    ProductWithOptionalDescription(
      product_id = 1,
      product_name = "myProductV4",
      product_price = 13.99,
      product_description = None
    )

  "adding mandatory field" - {

    srClient.deleteSubject(subjectName)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val productProducerRecord = new ProducerRecord[String, Product](topicName, "prodKey", product)
    productProducer.send(productProducerRecord).get()
    // subject has been deleted, so can only set compat after new schema has revived the subject
    srClient.setCompat(subjectName, "FORWARD")

    val productV2ProducerRecord =
      new ProducerRecord[String, ProductWithDescription](
        topicName,
        "prodKeyV2",
        productWithDescription
      )
    productWithDescriptionProducer.send(productV2ProducerRecord).get()

    "added mandatory field ignored by old consumer - forward compat lets OLD CONSUMER read old records and new records" in {

      productConsumer.subscribe(List(topicName).asJava)

      val records: Iterable[ConsumerRecord[String, Product]] =
        KafkaSpecHelper.fetchAndProcessRecords(productConsumer)

      val products: List[Product] = records.map(_.value()).toList
      products foreach println
      records.size mustBe 2
    }

    "added mandatory field required on old records by NEW CONSUMER  -> breaks on old records without added field" in {

      productWithDescriptionConsumer.subscribe(List(topicName).asJava)

      val records: Iterable[ConsumerRecord[String, ProductWithDescription]] =
        KafkaSpecHelper.fetchAndProcessRecords(productWithDescriptionConsumer)

      //java.lang.ClassCastException: class com.examples.schema.Product cannot be cast to class com.examples.schema.ProductV2
      // (com.examples.schema.Product and com.examples.schema.ProductV2 are in unnamed module of loader 'app')
      val ex = intercept[ClassCastException](records.map(_.value()).toList)
      println(ex)
      ex.getMessage.contains(
        "Product cannot be cast to class com.example.avro.specificrecords.Product"
      ) mustBe true
    }
  }

  // both tests for adding optional field
  "adding optional field" - {

    srClient.deleteSubject(subjectName)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val productProducerRecord = new ProducerRecord[String, Product](topicName, "prodKey", product)
    productProducer.send(productProducerRecord).get()
    // subject has been deleted, so can only set compat after new schema has revived the subject
    srClient.setCompat(subjectName, "FORWARD")

    val productOptProducerRecord =
      new ProducerRecord[String, ProductWithOptionalDescription](
        topicName,
        "prodKeyV2",
        productWithOptionalDescriptionPresent
      )
    productWithOptDescriptionProducer.send(productOptProducerRecord).get()

    "added optional field ignored by old consumer - forward compat lets OLD CONSUMER read old records and new records" in {

      productConsumer.subscribe(List(topicName).asJava)

      val records: Iterable[ConsumerRecord[String, Product]] =
        KafkaSpecHelper.fetchAndProcessRecords(productConsumer)

      val products: List[Product] = records.map(_.value()).toList
      products foreach println
      records.size mustBe 2
    }

    // TODO - this is wrong, only a casting issue due to naming - need to address
    "added optional field required on old records by NEW CONSUMER -> breaks on old records without added field" in {

      productWithOptDescriptionConsumer.subscribe(List(topicName).asJava)

      val records: Iterable[ConsumerRecord[String, ProductWithOptionalDescription]] =
        KafkaSpecHelper.fetchAndProcessRecords(productWithOptDescriptionConsumer)

      //java.lang.ClassCastException: class com.examples.schema.Product cannot be cast to class com.examples.schema.ProductV2
      // (com.examples.schema.Product and com.examples.schema.ProductV2 are in unnamed module of loader 'app')
      val ex = intercept[ClassCastException](records.map(_.value()).toList)
      println(ex)
      ex.getMessage.contains(
        "Product cannot be cast to class com.example.avro.specificrecords.Product"
      ) mustBe true
    }
  }

  // both tests for removing optional field

  // both tests for removing mandatory field

}
