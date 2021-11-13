package com.example.avro.enm

import com.example.avro.enm.Enums.Suit4
import com.example.{ SRBase, SpecBase }
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericData.{ EnumSymbol, Record }
import org.apache.avro.{ AvroTypeException, Schema }
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.common.errors.SerializationException

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AvroSchemaEnumCompatSpec extends SpecBase with SRBase {

  // we recommend against changing enums, explaining that this change is incompatible
  // it is not that clear-cut. There are some compatible changes, some incompatible changes and some gres area
  // however, as it is that complicated, you probably do not want to change your enums

  val subject: String = suiteName

  val fourEnumSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\r\n}"

  val fourEnumReverseSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"CLUBS\", \"DIAMONDS\", \"HEARTS\", \"SPADES\" ]\r\n}"

  val fourEnumReplaceSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"A\", \"B\", \"C\", \"D\" ]\r\n}"

  val threeEnumSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\"]\r\n}"

  val threeEnumWithDefaultSchemaString: String =
    "{ \"type\": \"enum\",\r\n \"name\": \"Suit\",\r\n \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\"], \r\n \"default\": \"HEARTS\"\r\n}"

  // fun quiz question - what is the return type of a method called isBackwardCompatible(ParsedSchema previousSchema)?
  // Boolean? wrong! its List<String>

  private val fieldName = "enumTest"
  val recordPrefix: String =
    "{ \"type\": \"record\",\r\n \"name\": \"" + fieldName + "\",\r\n \"namespace\": \"com.example\", \r\n \"fields\": [\r\n"
  val recordSuffix = "\n]\n}"

  val fieldPrefix: String = "{\"name\": \"" + fieldName + "\",\r\n \"type\": "
  val fieldSuffix         = "}"

  val fourElementsEnumSchema: Schema        = new Schema.Parser().parse(fourEnumSchemaString)
  val fourElementsReverseEnumSchema: Schema = new Schema.Parser().parse(fourEnumReverseSchemaString)
  val fourElementsReplaceEnumSchema: Schema = new Schema.Parser().parse(fourEnumReplaceSchemaString)
  val threeElementsEnumSchema: Schema       = new Schema.Parser().parse(threeEnumSchemaString)
  val threeElementsWithDefaultEnumSchema: Schema =
    new Schema.Parser().parse(threeEnumWithDefaultSchemaString)

  val fourElementsEnumAvroSchema             = new AvroSchema(fourElementsEnumSchema)
  val fourElementsReverseEnumAvroSchema      = new AvroSchema(fourElementsReverseEnumSchema)
  val fourElementsReplaceEnumAvroSchema      = new AvroSchema(fourElementsReplaceEnumSchema)
  val threeElementsEnumAvroSchema            = new AvroSchema(threeElementsEnumSchema)
  val threeElementsWithDefaultEnumAvroSchema = new AvroSchema(threeElementsWithDefaultEnumSchema)

  "deserializing" - {

    val mockSRClient = new MockSchemaRegistryClient()
    val serializer   = new TestSerializer(mockSRClient)
    val deserializer = new KafkaAvroDeserializer(mockSRClient)

    val recordSchemaFourEnum: Schema =
      new Schema.Parser().parse(makeSchemaString(fourEnumSchemaString))
    val recordAvroSchemaFourEnum = new AvroSchema(recordSchemaFourEnum)

    val recordSchemaThreeEnum: Schema =
      new Schema.Parser().parse(makeSchemaString(threeEnumSchemaString))

    val recordSchemaThreeEnumWithDefault: Schema =
      new Schema.Parser().parse(makeSchemaString(threeEnumWithDefaultSchemaString))

    val recordSchemaReverseEnum: Schema =
      new Schema.Parser().parse(makeSchemaString(fourEnumReverseSchemaString))

    // clubs is element Nr 4
    val symbolClubs: EnumSymbol =
      new EnumSymbol(fourElementsEnumSchema, Suit4.CLUBS)

    val recordClubs =
      new GenericRecordBuilder(recordSchemaFourEnum).set(fieldName, symbolClubs).build()
    val serializedClubs: Array[Byte] =
      serializer.serialize(subject, recordClubs, recordAvroSchemaFourEnum)

    "serialization stores index of enum" in {

      println(s"serialized Clubs ${serializedClubs.mkString("(", ",", ")")}")

      val symbolHearts: EnumSymbol =
        new EnumSymbol(fourElementsEnumSchema, Suit4.HEARTS)
      val recordHearts =
        new GenericRecordBuilder(recordSchemaFourEnum).set(fieldName, symbolHearts).build()
      val serializedHearts: Array[Byte] =
        serializer.serialize(subject, recordHearts, recordAvroSchemaFourEnum)
      println(s"serialized Hearts ${serializedHearts.mkString("(", ",", ")")}")

      val symbolDiamonds: EnumSymbol =
        new EnumSymbol(fourElementsEnumSchema, Suit4.DIAMONDS)
      val recordDiamonds =
        new GenericRecordBuilder(recordSchemaFourEnum).set(fieldName, symbolDiamonds).build()
      val serializedDiamonds: Array[Byte] =
        serializer.serialize(subject, recordDiamonds, recordAvroSchemaFourEnum)
      println(s"serialized Diamonds ${serializedDiamonds.mkString("(", ",", ")")}")

      val symbolSpades: EnumSymbol =
        new EnumSymbol(fourElementsEnumSchema, Suit4.SPADES)
      val recordSpades =
        new GenericRecordBuilder(recordSchemaFourEnum).set(fieldName, symbolSpades).build()
      val serializedSpades: Array[Byte] =
        serializer.serialize(subject, recordSpades, recordAvroSchemaFourEnum)
      println(s"serialized Spades ${serializedSpades.mkString("(", ",", ")")}")

    }

    "serializing & deserializing must work with same schema" in {

      val deserializedSameSchema: Record =
        deserializer
          .deserialize("useless_string_will_be_dropped", serializedClubs, recordSchemaFourEnum)
          .asInstanceOf[Record]
      val enField: EnumSymbol = deserializedSameSchema.get(fieldName).asInstanceOf[EnumSymbol]
      Suit4.valueOf(enField.toString) mustBe Suit4.CLUBS
    }

    "deserializing must work with reordered schema" in {
      val deserializedReverseSchema =
        deserializer
          .deserialize(
            "useless_string_will_be_dropped",
            serializedClubs,
            recordSchemaReverseEnum
          )
          .asInstanceOf[Record]
      val enField: EnumSymbol = deserializedReverseSchema.get(fieldName).asInstanceOf[EnumSymbol]
      Suit4.valueOf(enField.toString) mustBe Suit4.CLUBS
    }

    "deserializing must fail if symbol is removed" in {
      val deserializedThreeSchema: SerializationException =
        intercept[SerializationException] {
          deserializer.deserialize(
            "useless_string_will_be_dropped",
            serializedClubs,
            recordSchemaThreeEnum
          )
        }
      val cause: Throwable = deserializedThreeSchema.getCause
      cause mustBe a[AvroTypeException]
      cause.getMessage mustBe "No match for CLUBS"
    }

    "deserializing will use default value if symbol is removed but a default exists" in {
      val deserializedThreeSchema =
        deserializer
          .deserialize(
            "useless_string_will_be_dropped",
            serializedClubs,
            recordSchemaThreeEnumWithDefault
          )
          .asInstanceOf[Record]
      val enField: EnumSymbol = deserializedThreeSchema.get(fieldName).asInstanceOf[EnumSymbol]
      Suit4.valueOf(enField.toString) mustBe Suit4.HEARTS
    }

  }

  "testing compatibility" - {

    // can consumer read old data with new schema? NO, a symbol is missing
    "removing element from enum must be incompatible" in {
      // Checking compatibility of reader {"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS"]} with writer {"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]}
      // Incompatibility{type:MISSING_ENUM_SYMBOLS, location:/symbols, message:[CLUBS], reader:{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS"]}, writer:{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","DIAMONDS","CLUBS"]}}
      val result: mutable.Buffer[String] =
        threeElementsEnumAvroSchema.isBackwardCompatible(fourElementsEnumAvroSchema).asScala
      result.head.contains("MISSING_ENUM_SYMBOLS") mustBe true
    }

    // and removing an element is forwards compatible
    "adding element must be backwards compatible" in {
      val res2: mutable.Buffer[String] =
        fourElementsEnumAvroSchema.isBackwardCompatible(threeElementsEnumAvroSchema).asScala
      res2.isEmpty mustBe true
    }

    "with defaults, removing elements must be backwards compatible " in {

      val result: mutable.Buffer[String] =
        threeElementsWithDefaultEnumAvroSchema
          .isBackwardCompatible(fourElementsEnumAvroSchema)
          .asScala
      result.isEmpty mustBe true
    }

    // you dont even need the defaults, but just to be sure
    "with defaults, adding elements must be backwards compatible " in {
      val result: mutable.Buffer[String] =
        fourElementsEnumAvroSchema
          .isBackwardCompatible(threeElementsWithDefaultEnumAvroSchema)
          .asScala
      result.isEmpty mustBe true
    }

    "changing sequence must be compatible" in {
      val result: mutable.Buffer[String] =
        fourElementsReverseEnumAvroSchema
          .isBackwardCompatible(fourElementsEnumAvroSchema)
          .asScala
      result.isEmpty mustBe true
    }

    "using same number of different elements must be incompatible" in {
      val result: mutable.Buffer[String] =
        fourElementsReplaceEnumAvroSchema
          .isBackwardCompatible(fourElementsEnumAvroSchema)
          .asScala
      result.head.contains("MISSING_ENUM_SYMBOLS") mustBe true
    }
  }

  private def makeSchemaString(fieldSchema: String) =
    recordPrefix + fieldPrefix + fieldSchema + fieldSuffix + recordSuffix

}
