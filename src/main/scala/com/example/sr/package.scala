package com.example

package object sr {

  case class Order(id: String, itemId: String, customerId: String, quantity: Int)
  case class Item(id: String, name: String, price: Int)
  case class Customer(id: String, name: String)

  case class SchemaEnvelope(
      subject: String,
      schema: String,
      version: Option[Int] = None,
      id: Option[Int] = None
  )
  case class PostSchemaEnvelope(schema: String)

}
