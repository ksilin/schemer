package com.example.sr

import com.example.SpecBase
import com.example.avro.{ SrRestClient, SrRestConfig }

import scala.jdk.CollectionConverters._
import better.files._
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference

import scala.util.Try

class SrRestClientSpec extends SpecBase {

  val srConfig: SrRestConfig = SrRestConfig(srUrl, s"$srKey:$srSecret")

  val client: SrRestClient = SrRestClient(srConfig)

  "must get all subjects" in {
    val subjects = client.schemaRegistryClient.getAllSubjects.asScala
    subjects foreach (s => info(s))
  }

  "must register a schema" in {

    val schemaPath = "avro/Customer.avsc"
    val customerSchema: Either[String, String] =
      Resource.asString(schemaPath).toRight(s"failed to read schema $schemaPath")
    val res: Either[String, Int] = customerSchema.flatMap { schemaString =>
      client.register("customer-srtest", schemaString)
    }
    res mustBe a[Right[String, Int]]

    // now fetch the schema and compare:

    res.map { schemaId =>
      val schema: Try[ParsedSchema] = Try(client.schemaRegistryClient.getSchemaById(schemaId))
      println(s"found schema with ID $schemaId:")
      println(schema)
    }
  }

  "must register a schema with references" in {

    val customerSchemaPath = "avro/Customer.avsc"
    val customerSchema: Either[String, String] =
      Resource.asString(customerSchemaPath).toRight(s"failed to read schema $customerSchemaPath")
    val customerSchemaRegistered: Either[String, Int] = customerSchema.flatMap { schemaString =>
      client.register("customer", schemaString)
    }
    customerSchemaRegistered mustBe a[Right[String, Int]]

    val productSchemaPath = "avro/Product.avsc"
    val productSchema: Either[String, String] =
      Resource.asString(productSchemaPath).toRight(s"failed to read schema $productSchemaPath")
    val productSchemaRegistered: Either[String, Int] = productSchema.flatMap { schemaString =>
      client.register("product", schemaString)
    }
    productSchemaRegistered mustBe a[Right[String, Int]]

    // without the references:
    // FAILs [ERROR] [AvroSchemaProvider] - Could not parse Avro schema
    //org.apache.avro.SchemaParseException: Undefined name: "io.confluent.examples.avro.Customer"

    val references = List(
      new SchemaReference("io.confluent.examples.avro.Customer", "customer", 1),
      new SchemaReference("io.confluent.examples.avro.Product", "product", 1)
    )

    val unionSchemaPath = "avro/AllOf.avsc"
    val unionSchema: Either[String, String] =
      Resource.asString(unionSchemaPath).toRight(s"failed to read schema $unionSchemaPath")
    val unionSchemaRegistered: Either[String, Int] = unionSchema.flatMap { schemaString =>
      client.register("union-srtest", schemaString, references = references)
    }
    unionSchemaRegistered mustBe a[Right[String, Int]]
  }

}
