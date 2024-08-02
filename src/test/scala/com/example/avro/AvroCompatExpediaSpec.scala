package com.example.avro

import com.example.util.{ SRBase, SpecBase }
import com.hotels.avro.compatibility.Compatibility.CheckType
import com.hotels.avro.compatibility.{ Compatibility, CompatibilityCheckResult }
import org.apache.avro.SchemaCompatibility.{ SchemaCompatibilityType, SchemaIncompatibilityType }
import org.apache.avro.{ Schema, SchemaCompatibility }

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AvroCompatExpediaSpec extends SpecBase with SRBase {

  val fourEnumSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\r\n}"

  val threeEnumSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\"]\r\n}"

  val fourElementsEnumSchema: Schema =
    new Schema.Parser().parse(fourEnumSchemaString)
  val threeElementsEnumSchema: Schema =
    new Schema.Parser().parse(threeEnumSchemaString)

  "reader removing element from enum is not compatible" in {

    val res1: CompatibilityCheckResult =
      Compatibility.checkThat(fourElementsEnumSchema).canBeReadBy(threeElementsEnumSchema)
    res1.isCompatible mustBe false
    res1.getResult.getCompatibility mustBe SchemaCompatibilityType.INCOMPATIBLE
    println("result incompatibilities:")
    val incompatibilities: mutable.Buffer[SchemaCompatibility.Incompatibility] =
      res1.getResult.getIncompatibilities.asScala
    incompatibilities.size mustBe 1
    // one of NAME_MISMATCH, FIXED_SIZE_MISMATCH, MISSING_ENUM_SYMBOLS, READER_FIELD_MISSING_DEFAULT_VALUE, TYPE_MISMATCH, MISSING_UNION_BRANCH
    incompatibilities.head.getType mustBe SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS

    println("description:")
    println(res1.getDescription)
    // println(res1.getChronology)
  }

  "reader adding element to enum is fine" in {
    val canFourElementSchemaReadThreeElementEnum: CompatibilityCheckResult =
      Compatibility.checkThat(threeElementsEnumSchema).canBeReadBy(fourElementsEnumSchema)
    canFourElementSchemaReadThreeElementEnum.getCompatibility mustBe CheckType.CAN_BE_READ_BY

  }

}
