package com.example

import com.examples.schema.Customer
import com.examples.schema.Product

package object avro {

  case class SimpleTestData(id: Int, data: String)

  // copied from files generated  by avrohugger: compile / avroScalaGenerate
  // NS is io.confluent.examples.avro

//  final case class Product(product_id: Int, product_name: String, product_price: Double)
//
//  final case class Customer(
//      customer_id: Int,
//      customer_name: String,
//      customer_email: String,
//      customer_address: String
//  )
//
//  // Either wirks with two options, what about more?
  final case class AllTypes(oneof_type: Either[Customer, Product])

}
