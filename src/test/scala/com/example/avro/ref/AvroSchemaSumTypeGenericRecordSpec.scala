package com.example.avro.ref

import com.example.avro.CustomerOrProductCase
import com.example.{ KafkaSpecHelper, LocalSchemaCoordinates, SpecBase }
import com.examples.schema.{ Customer, Product }
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericRecordBuilder }
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerRecord, RecordMetadata }

import scala.jdk.CollectionConverters._

class AvroSchemaSumTypeGenericRecordSpec extends SpecBase {

  val topicName: String = suiteName

  val customerSchemaPath = "avro/Customer.avsc"
  val productSchemaPath  = "avro/Product.avsc"

  // avrohugger cannot generate a SpecificRecord for the AllOF type and fails,
  // so moving out of generator path to let it work on th rest
  val customerOrProductSchemaPath = "manual_avro/CustomerOrProduct.avsc"
  val customerOrProductInlineSchemaPath = "manual_avro/CustomerOrProductInline.avsc"

  val productSubject  = s"product-$suiteName"
  val customerSubject = s"customer-$suiteName"
  // TODO: show how naming strategies work in resolving the subject name
  val subjectName = s"$topicName-value"
  //val subjectName    = s"customerOrProduct-$suiteName"

  val customerSchemaCoord = LocalSchemaCoordinates(customerSchemaPath, customerSubject)
  val productSchemaCoord  = LocalSchemaCoordinates(productSchemaPath, productSubject)
  val customerOrProductSchemaCoord =
    LocalSchemaCoordinates(customerOrProductSchemaPath, subjectName)
  val customerOrProductInlineSchemaCoord =
    LocalSchemaCoordinates(customerOrProductSchemaPath, subjectName)
  val schemasToDelete   = List(customerSchemaCoord, productSchemaCoord, customerOrProductSchemaCoord)
  val schemasToRegister = List(customerSchemaCoord, productSchemaCoord)

  val producerSumType: Producer[String, CustomerOrProductCase] =
    new KafkaProducer[String, CustomerOrProductCase](props)
  val producer: Producer[String, GenericRecord] = new KafkaProducer[String, GenericRecord](props)
  val consumer: Consumer[String, GenericRecord] = new KafkaConsumer[String, GenericRecord](props)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 12.99)
  val customer: Customer = Customer(
    customer_id = 1,
    customer_name = "Kun De",
    customer_email = "kunde@mailinator.com",
    customer_address = "Fake Street 123"
  )

  "must write and read avro with references" in {

    prepSchemas(schemasToDelete, schemasToRegister)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val references = List(
      new SchemaReference("com.examples.schema.Customer", customerSubject, 1),
      new SchemaReference("com.examples.schema.Product", productSubject, 1)
    )

    val sumTypeSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(
        customerOrProductSchemaPath,
        subjectName,
        references
      ) //allOfSubject, references)

    val id                    = sumTypeSchemaRegistered.right.get
    val sumTypeSchema: Schema = srClient.schemaRegistryClient.getById(id)

    println("customerOrProduct Schema: ")
    println(sumTypeSchema)

    // the schema from SR does actually inline the references too:
    // {"type":"record","name":"AllTypes","namespace":"com.examples.schema","fields":[{"name":"oneof_type","type":[{"type":"record","name":"Customer","fields":[{"name":"customer_id","type":"int"},{"name":"customer_name","type":"string"},{"name":"customer_email","type":"string"},{"name":"customer_address","type":"string"}]},{"type":"record","name":"Product","fields":[{"name":"product_id","type":"int"},{"name":"product_name","type":"string"},{"name":"product_price","type":"double"}]}]}]}

    // original schema cannot be parsed, as the references cannot be resolved
    // val schemaOrError: Either[String, String] =
    //  Resource.asString(allOfSchemaPath).toRight(s"failed to read schema $allOfSchemaPath")
    // schemaOrError mustBe a[Right[String, String]]
    // val sumTypeSchema: Schema = new org.apache.avro.Schema.Parser().parse(schemaOrError.right.get)

    consumer.subscribe(List(topicName).asJava)

    val withProductBuilder = new GenericRecordBuilder(sumTypeSchema)
    withProductBuilder.set("customerOrProduct", product)
    val productProducerRecord =
      new ProducerRecord[String, GenericRecord](
        topicName,
        "productKey",
        withProductBuilder.build()
      )

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent1: RecordMetadata = producer.send(productProducerRecord).get()

    val withCustomerBuilder = new GenericRecordBuilder(sumTypeSchema)
    withCustomerBuilder.set("customerOrProduct", customer)
    val customerRecord: GenericData.Record = withCustomerBuilder.build()

    val customerProducerRecord =
      new ProducerRecord[String, GenericRecord](topicName, "customerKey", customerRecord)

    // ERROR: Unknown datum type com.examples.schema.Product: Product(1,myProduct,12.99)
    val sent2: RecordMetadata = producer.send(customerProducerRecord).get()
    producer.close()

    val records: Iterable[ConsumerRecord[String, GenericRecord]] =
      KafkaSpecHelper.fetchAndProcessRecords(consumer)

    records.size mustBe 2
  }

}
