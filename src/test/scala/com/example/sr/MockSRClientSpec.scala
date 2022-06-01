package com.example.sr

import com.example.avro.specificrecords.Product
import com.example.{ KafkaSpecHelper, SpecBase }
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.{
  AbstractKafkaSchemaSerDeConfig,
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializer
}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }

import java.util.Properties
import scala.jdk.CollectionConverters._

class MockSRClientSpec extends SpecBase {

  private val SCHEMA_REGISTRY_SCOPE    = suiteName
  private val MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE

  val specProps = new Properties()
  specProps.putAll(props)
  specProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
  // without it, the event type is looked up in the subject and fails
  specProps.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false)
  specProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL)
  specProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)

  val topic = s"$suiteName-topic"
  KafkaSpecHelper.createOrTruncateTopic(adminClient, topic)

  val product = Product(123, "prodName", 7.99)

  "with explicit client" - {

    val keySerializer = new StringSerializer()

    val schemaRegistryClient: SchemaRegistryClient =
      MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE)

    val mockSRAvroSerializer: KafkaAvroSerializer = new KafkaAvroSerializer(
      schemaRegistryClient,
      specProps.asInstanceOf[java.util.Map[String, String]]
    )

    val keyDeserializer = new StringDeserializer()
    // TODO - how to use specific reader and have proper V type?
    val deserializer: KafkaAvroDeserializer = new KafkaAvroDeserializer(
      schemaRegistryClient,
      specProps.asInstanceOf[java.util.Map[String, String]]
    )

    "must register schemas, serialize and deserialize directly" in {}

    "must be able to be configured for Producer and Consumer " in {

      // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG - accepts only class, not an instance
      val p: KafkaProducer[String, AnyRef] =
        new KafkaProducer(specProps, keySerializer, mockSRAvroSerializer)

      val record =
        new ProducerRecord[String, AnyRef](topic, "prodKey", product) //.asInstanceOf[Object])
      val sent: RecordMetadata = p.send(record).get()
      println(sent)

      val c = new KafkaConsumer[String, AnyRef](specProps, keyDeserializer, deserializer)
      c.subscribe(List(topic).asJava)

      val recs = KafkaSpecHelper.fetchAndProcessRecords(c)
      recs foreach println
      val rec = recs.head.value().asInstanceOf[GenericRecord]
      rec.getSchema.getFields.asScala foreach println
    }
  }

  "with a mock:// SR URL" - {
    "must be able to be configured for Producer and Consumer " in {

      // the consumer itself does sth else:
      // it generates a ConsumerConfig and uses it to create instances
      // if (valueDeserializer == null) {
      // this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class)
      // this.valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)),

      // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG - accepts only class, not an instance
      val p: KafkaProducer[String, Product] =
        new KafkaProducer(specProps)

      val record =
        new ProducerRecord[String, Product](topic, "prodKey", product) //.asInstanceOf[Object])
      val sent: RecordMetadata = p.send(record).get()
      println(sent)

      val c = new KafkaConsumer[String, Product](specProps)
      c.subscribe(List(topic).asJava)

      val recs = KafkaSpecHelper.fetchAndProcessRecords(c)
      recs foreach println
      val rec: Product = recs.head.value()
      println(rec)
    }
  }

  // ---

}
