/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.example.avro.specificrecords

import scala.annotation.switch

final case class ProductWithDescription(
    var product_id: Int,
    var product_name: String,
    var product_price: Double,
    var product_description: String
) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(0, "", 0.0, "")
  def get(field$ : Int): AnyRef =
    (field$ : @switch) match {
      case 0 =>
        product_id
          .asInstanceOf[AnyRef]
      case 1 =>
        product_name
          .asInstanceOf[AnyRef]
      case 2 =>
        product_price
          .asInstanceOf[AnyRef]
      case 3 =>
        product_description
          .asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.product_id = value
          .asInstanceOf[Int]
      case 1 =>
        this.product_name = value.toString
          .asInstanceOf[String]
      case 2 =>
        this.product_price = value
          .asInstanceOf[Double]
      case 3 =>
        this.product_description = value.toString
          .asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = ProductWithDescription.SCHEMA$
}

object ProductWithDescription {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Product\",\"namespace\":\"com.example.avro.specificrecords\",\"fields\":[{\"name\":\"product_id\",\"type\":\"int\"},{\"name\":\"product_name\",\"type\":\"string\"},{\"name\":\"product_price\",\"type\":\"double\"},{\"name\":\"product_description\",\"type\":\"string\"}]}"
  )
}
