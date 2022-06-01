package com.example.sr

import com.example.avro.specificrecords.Product
import io.confluent.kafka.serializers.{ KafkaAvroDeserializer, KafkaAvroDeserializerConfig }
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class CustomProductDeserializer extends Deserializer[Product] {

  var inner: KafkaAvroDeserializer = new KafkaAvroDeserializer()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(configs, isKey)
    val scProps = configs.asScala.asInstanceOf[mutable.Map[String, Any]]
    scProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, java.lang.Boolean.FALSE)
    //false)
    inner.configure(scProps.asJava, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): Product = {
    if (data == null) return null
    val gRecord: GenericRecord = inner.deserialize(topic, data).asInstanceOf[GenericRecord]
    println(s"found record: $gRecord")
    println("record fields:")
    gRecord.getSchema.getFields.asScala foreach println
    Product(
      gRecord.get("product_id").asInstanceOf[Int],
      // cannot just cast to String
      // org.apache.kafka.common.errors.SerializationException: Error deserializing key/value for partition CustomDeserializerSpec-0 at offset 0. If needed, please seek past the record to continue consumption.
      // Caused by: java.lang.ClassCastException: class org.apache.avro.util.Utf8 cannot be cast to class java.lang.String (org.apache.avro.util.Utf8 is in unnamed module of loader 'app'; java.lang.String is in module java.base of loader 'bootstrap')
      gRecord.get("product_name").toString,
      gRecord.get("product_price").asInstanceOf[Double]
    )
  }
}
