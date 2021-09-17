package com.example.sr

import com.example.SpecBase

import scala.jdk.CollectionConverters._
import better.files._
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference

import scala.util.Try

class SrRestClientSpec extends SpecBase {

  val customerSchemaPath = "avro/Customer.avsc"
  val productSchemaPath  = "avro/Product.avsc"

  // avrohugger cannot generate a SpecificRecord for the AllOF type and fails,
  // so moving out of generator path to let it work on th rest
  val allOfSchemaPath = "manual_avro/CustomerOrProduct.avsc"

  val productSubject  = s"product-$suiteName"
  val customerSubject = s"customer-$suiteName"
  val allOfSubject    = s"allOf-$suiteName"

  "must get all subjects" in {
    val subjects = srClient.schemaRegistryClient.getAllSubjects.asScala
    subjects foreach (s => info(s))
  }

  "must register a schema" in {
    srClient.deleteSubjects(List(allOfSubject, customerSubject))

    val customerSchema: Either[String, String] =
      Resource.asString(customerSchemaPath).toRight(s"failed to read schema $customerSchemaPath")
    val res: Either[String, Int] = customerSchema.flatMap { schemaString =>
      srClient.register(customerSubject, schemaString)
    }
    res mustBe a[Right[String, Int]]

    res.map { schemaId =>
      val schema: Try[ParsedSchema] = Try(srClient.schemaRegistryClient.getSchemaById(schemaId))
      println(s"found schema with ID $schemaId:")
      println(schema)
    }
  }

  "must register a schema with references" in {

    srClient.deleteSubjects(List(allOfSubject, customerSubject, productSubject))

    val customerSchemaRegistered: scala.Either[String, Int] =
      srClient.registerSchemaFromResource(customerSchemaPath, customerSubject)
    customerSchemaRegistered mustBe a[Right[String, Int]]

    val productSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(productSchemaPath, productSubject)
    productSchemaRegistered mustBe a[Right[String, Int]]

    // without the references:
    // FAILs [ERROR] [AvroSchemaProvider] - Could not parse Avro schema
    //org.apache.avro.SchemaParseException: Undefined name: "io.confluent.examples.avro.Customer"
    val references = List(
      new SchemaReference("com.examples.schema.Customer", customerSubject, 1),
      new SchemaReference("com.examples.schema.Product", productSubject, 1)
    )

    val unionSchemaRegistered: Either[String, Int] =
      srClient.registerSchemaFromResource(allOfSchemaPath, allOfSubject, references)
    unionSchemaRegistered mustBe a[Right[String, Int]]
    unionSchemaRegistered.map { schemaId =>
      val schema: Try[ParsedSchema] = Try(srClient.schemaRegistryClient.getSchemaById(schemaId))
      println(s"found union schema with ID $schemaId:")
      println(schema)
    }
  }

}
