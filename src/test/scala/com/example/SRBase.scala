package com.example

import com.example.avro.SrRestClient
import org.scalatest.matchers.must.Matchers

case class LocalSchemaCoordinates(schemaPath: String, subject: String)
case class RemoteSchemaCoordinates(
    subject: String,
    version: Option[Int] = None,
    id: Option[Int] = None
)

trait SRBase extends Matchers {

  val srConfig: SrRestProps  = SrRestProps.create()
  val srClient: SrRestClient = SrRestClient(srConfig)

  // TODO - // schemas to delete should be RemoteSchemaCoordinates
  def prepSchemas(
      schemasToDelete: List[LocalSchemaCoordinates],
      schemasToRegister: List[LocalSchemaCoordinates]
  ): List[(LocalSchemaCoordinates, Either[String, Int])] = {

    srClient.deleteSubjects(schemasToDelete.map(_.subject))

    schemasToRegister map { s =>
      val schemaRegistered: Either[String, Int] =
        srClient.registerSchemaFromResource(s.schemaPath, s.subject)
      schemaRegistered mustBe a[Right[_, _]]
      s -> schemaRegistered
    }
  }
}
