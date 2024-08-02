package com.example.avro

import com.example.util.{ SRBase, SpecBase }
import io.confluent.kafka.schemaregistry.CompatibilityLevel
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.{ Schema, SchemaBuilder }

import scala.jdk.CollectionConverters._

class AvroSchemaBuilderSpec extends SpecBase with SRBase {

  val namespace                                            = "com.testing"
  val namespacedBuilder: SchemaBuilder.TypeBuilder[Schema] = SchemaBuilder.builder(namespace)

  "building a simple schema" in {
    val recordBuilder: SchemaBuilder.RecordBuilder[Schema]   = namespacedBuilder.record("testRecord")
    val fieldAssembler: SchemaBuilder.FieldAssembler[Schema] = recordBuilder.fields()

    // name(fieldName).type().optional().stringType()
    val schema: Schema = fieldAssembler
      .optionalString("title")
      .nullableString("desc", "DEFAULT_DESC")
      .requiredString("name")
      .endRecord()

    //{
    //  "type":"record",
    //  "name":"testRecord",
    //  "namespace":"com.testing",
    //  "fields":[
    //    {
    //      "name":"title",
    //      "type":[
    //        "null",
    //        "string"
    //      ],
    //      "default":null
    //    },
    //    {
    //      "name":"desc",
    //      "type":[
    //        "string",
    //        "null"
    //      ],
    //      "default":"DEFAULT_DESC"
    //    },
    //    {
    //      "name":"name",
    //      "type":"string"
    //    }
    //  ]
    //}
    println(schema)
  }

  "parsed and built schemas must be same" in {

    val testProduct = "TestProduct"

    val schemaString = s"""{
      "type": "record",
      "namespace": "$namespace",
      "name": "$testProduct",

      "fields": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "price", "type": "double"}
      ]
    }"""

    val fieldAssembler = namespacedBuilder.record(testProduct).fields()
    val schema: Schema = fieldAssembler
      .requiredInt("id")
      .requiredString("name")
      .requiredDouble("price")
      .endRecord()

    val SCHEMA: Schema = new org.apache.avro.Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"TestProduct\",\"namespace\":\"com.testing\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"}]}"
    )

    val SCHEMA2: Schema = new org.apache.avro.Schema.Parser().parse(schemaString)

    // schemas are not same
    SCHEMA.equals(SCHEMA2) mustBe true
    SCHEMA.equals(schema) mustBe true

    val avro1 = new AvroSchema(SCHEMA)
    val avro2 = new AvroSchema(SCHEMA2)
    val avro3 = new AvroSchema(schema)

    avro1.isCompatible(CompatibilityLevel.FULL, List(avro2).asJava).isEmpty mustBe true
    avro1.isCompatible(CompatibilityLevel.FULL, List(avro3).asJava).isEmpty mustBe true
  }

  "building a schema with an enum" in {

    val enumSchema: Schema =
      SchemaBuilder
        .enumeration("enumName")
        .namespace(namespace)
        .symbols("a", "b", "c", "d", "e")

    val myschema: Schema =
      SchemaBuilder
        .record("recordSchema")
        .namespace(namespace)
        .fields()
        .name("enumTest")
        .`type`()
        .unionOf()
        .nullType()
        .and()
        .`type`(enumSchema)
        .endUnion()
        .noDefault()
        .endRecord()

    println(enumSchema)
    println(myschema)
  }

  "using schema to serialize and deserialize " in {}

}
