package com.example.avro.ref

import better.files.Resource
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

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.{ ListHasAsScala, SeqHasAsJava }

class AvroSchemaRefSpec extends SpecBase {

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

  "must write and read avro with references" in {

    val topicName = "avroRefSpec"
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val prefix = "avroRefSpec"
//    val schemas: List[ParsedSchema] = srClient.schemaRegistryClient
//      .getSchemas(prefix, false, true)
//      .asScala
//      .toList

    val references = List(
      new SchemaReference("com.examples.schema.Customer", "customer", 1),
      new SchemaReference("com.examples.schema.Product", "product", 1)
    )

    // val unionSchemaPath = "com/example/avro/AllOf.avsc"
    val unionSchemaPath                   = "tmp/AllOf.avsc"
    val maybeSchemaString: Option[String] = Resource.asString(unionSchemaPath)

    val unionSchema: Either[String, String] =
      maybeSchemaString.toRight(s"failed to read schema $unionSchemaPath")
    val unionSchemaRegistered: Either[String, Int] = unionSchema.flatMap { schemaString =>
      srClient.register("avroRefSpec-value", schemaString, references = references)
    }
    val id = unionSchemaRegistered.right.get

    val schema: Schema = srClient.schemaRegistryClient.getById(id)
    println("schema fields:")
    schema.getFields.asScala foreach println

//    schemas.size mustBe 1
//    val schema: ParsedSchema = schemas.head

    consumer.subscribe(List(topicName).asJava)

    val product                 = Product(product_id = 1, product_name = "myProduct", product_price = 12.99)
    val oneOfWithProductBuilder = new GenericRecordBuilder(schema)
    oneOfWithProductBuilder.set("oneof_type", product)
    val productRecord: GenericData.Record = oneOfWithProductBuilder.build()

    // val value   = AllTypes(Right(product))
    // val record = new ProducerRecord[String, AllTypes](topicName, "testKey", value)
    val productProducerRecord =
      new ProducerRecord[String, GenericRecord](topicName, "productKey", productRecord)

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent1: RecordMetadata = producer.send(productProducerRecord).get()
    println(sent1)

    val customer = Customer(
      customer_id = 1,
      customer_name = "Kun De",
      customer_email = "kunde@mailinator.com",
      customer_address = "Fake Street 123"
    )
    val oneOfWithCustomerBuilder = new GenericRecordBuilder(schema)
    oneOfWithCustomerBuilder.set("oneof_type", customer)
    val customerRecord: GenericData.Record = oneOfWithCustomerBuilder.build()

    val customerProducerRecord =
      new ProducerRecord[String, GenericRecord](topicName, "customerKey", customerRecord)

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent2: RecordMetadata = producer.send(customerProducerRecord).get()
    println(sent2)

    producer.close()

    val records: Iterable[ConsumerRecord[String, GenericRecord]] =
      KafkaSpecHelper.fetchAndProcessRecords(consumer)

  }

}
