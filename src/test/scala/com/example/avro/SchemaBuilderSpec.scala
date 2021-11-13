package com.example.avro

import com.example.SpecBase
import org.apache.avro.{ Schema, SchemaBuilder }

class SchemaBuilderSpec extends SpecBase {

  "generating a field and a full record schema must work" in {

    val namespace = "com.testing"

    val enumSchema: Schema =
      SchemaBuilder
        .enumeration("aname")
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

}
