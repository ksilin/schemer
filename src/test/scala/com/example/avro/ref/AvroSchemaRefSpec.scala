package com.example.avro.ref

import com.example.avro.{ AllTypes, SrRestClient, SrRestConfig }
import com.example.{ KafkaSpecHelper, SpecBase }
import com.examples.schema.{ Customer, Product }
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericRecordBuilder }
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  Producer,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}

import java.util.Properties
import scala.jdk.CollectionConverters._

class AvroSchemaRefSpec extends SpecBase {

  val customerSchemaPath = "avro/Customer.avsc"
  val productSchemaPath  = "avro/Product.avsc"

  // avrohugger cannot generate a SpecificRecord for the AllOF type and fails,
  // so moving out of generator path to let it work on th rest
  val allOfSchemaPath = "tmp/AllOf.avsc"

  val productSubject  = s"product-$suiteName"
  val customerSubject = s"customer-$suiteName"
  //val allOfSubject    = s"allOf-$suiteName"

  val props: Properties = config.commonProps.clone().asInstanceOf[Properties]

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

  val producerAllTypes: Producer[String, AllTypes] = new KafkaProducer[String, AllTypes](props)
  val producer: Producer[String, GenericRecord]    = new KafkaProducer[String, GenericRecord](props)
  //val consumer: Consumer[String, AllTypes] = new KafkaConsumer[String, AllTypes](props)
  val consumer: Consumer[String, GenericRecord] = new KafkaConsumer[String, GenericRecord](props)

  val srConfig: SrRestConfig = SrRestConfig(config.srUrl, s"${config.srKey}:${config.srSecret}")
  val srClient: SrRestClient = SrRestClient(srConfig)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 12.99)
  val customer: Customer = Customer(
    customer_id = 1,
    customer_name = "Kun De",
    customer_email = "kunde@mailinator.com",
    customer_address = "Fake Street 123"
  )

  "must write and read avro with references" in {

    // TODO: show how naming strategies work in resolving the subject name
    val topicName   = suiteName
    val subjectName = s"$topicName-value"

    prepSchemas(List(customerSubject, productSubject, subjectName))

    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val references = List(
      new SchemaReference("com.examples.schema.Customer", customerSubject, 1),
      new SchemaReference("com.examples.schema.Product", productSubject, 1)
    )

    val unionSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(
        allOfSchemaPath,
        subjectName,
        references
      ) //allOfSubject, references)
    val id = unionSchemaRegistered.right.get

    // TODO - how can I get a schema without fetching from SR?
    val allOfSchema: Schema = srClient.schemaRegistryClient.getById(id)

    consumer.subscribe(List(topicName).asJava)

    val oneOfWithProductBuilder = new GenericRecordBuilder(allOfSchema)
    oneOfWithProductBuilder.set("oneof_type", product)
    val productProducerRecord =
      new ProducerRecord[String, GenericRecord](
        topicName,
        "productKey",
        oneOfWithProductBuilder.build()
      )

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent1: RecordMetadata = producer.send(productProducerRecord).get()

    val oneOfWithCustomerBuilder = new GenericRecordBuilder(allOfSchema)
    oneOfWithCustomerBuilder.set("oneof_type", customer)
    val customerRecord: GenericData.Record = oneOfWithCustomerBuilder.build()

    val customerProducerRecord =
      new ProducerRecord[String, GenericRecord](topicName, "customerKey", customerRecord)

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent2: RecordMetadata = producer.send(customerProducerRecord).get()
    producer.close()

    val records: Iterable[ConsumerRecord[String, GenericRecord]] =
      KafkaSpecHelper.fetchAndProcessRecords(consumer)

    records.size mustBe 2
  }

  private def prepSchemas(schemasToDelete: List[String]) = {
    srClient.deleteSubjects(schemasToDelete)

    val customerSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(customerSchemaPath, customerSubject)
    customerSchemaRegistered mustBe a[Right[String, Int]]

    val productSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(productSchemaPath, productSubject)
    productSchemaRegistered mustBe a[Right[String, Int]]
  }
}
