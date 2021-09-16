package com.example.avro.ref

import com.example.{ KafkaSpecHelper, LocalSchemaCoordinates, SpecBase }
import com.examples.schema.{ Customer, Order, Product }
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.kafka.clients.consumer.{ Consumer, ConsumerRecord, KafkaConsumer }
import org.apache.kafka.clients.producer._

import scala.jdk.CollectionConverters._

class AvroSchemaProductTypeSpec extends SpecBase {

  // TODO: show how naming strategies work in resolving the subject name
  val topicName: String = suiteName

  val customerSchemaPath = "avro/Customer.avsc"
  val productSchemaPath  = "avro/Product.avsc"
  val orderSchemaPath    = "avro/Order.avsc"

  val productSubject  = s"product-$suiteName"
  val customerSubject = s"customer-$suiteName"
  // val orderSubject    = s"order-$suiteName"
  val orderSubject: String = s"$topicName-value"

  val customerSchemaCoord = LocalSchemaCoordinates(customerSchemaPath, customerSubject)
  val productSchemaCoord  = LocalSchemaCoordinates(productSchemaPath, productSubject)
  val orderSchemaCoord    = LocalSchemaCoordinates(orderSchemaPath, orderSubject)
  val schemasToDelete     = List(customerSchemaCoord, productSchemaCoord, orderSchemaCoord)
  val schemasToRegister   = List(customerSchemaCoord, productSchemaCoord)

  val producer: Producer[String, Order] = new KafkaProducer[String, Order](props)
  val consumer: Consumer[String, Order] = new KafkaConsumer[String, Order](props)
  consumer.subscribe(List(topicName).asJava)

  val product: Product = Product(product_id = 1, product_name = "myProduct", product_price = 12.99)
  val customer: Customer = Customer(
    customer_id = 1,
    customer_name = "Kun De",
    customer_email = "kunde@mailinator.com",
    customer_address = "Fake Street 123"
  )
  val order: Order = Order(customer, product)

  "must write and read avro with references" in {

    prepSchemas(schemasToDelete, schemasToRegister)
    KafkaSpecHelper.createOrTruncateTopic(adminClient, topicName)

    val references = List(
      new SchemaReference("com.examples.schema.Customer", customerSubject, 1),
      new SchemaReference("com.examples.schema.Product", productSubject, 1)
    )

    val orderSchemaRegistered: Either[String, Int] = srClient.registerSchemaFromResource(
      orderSchemaPath,
      orderSubject,
      references
    )
    orderSchemaRegistered mustBe a[Right[String, Int]]

    val productProducerRecord = new ProducerRecord[String, Order](topicName, "orderKey", order)

    producer.send(productProducerRecord).get()

    val records: Iterable[ConsumerRecord[String, Order]] =
      KafkaSpecHelper.fetchAndProcessRecords(consumer)

    records.size mustBe 1
  }

}
