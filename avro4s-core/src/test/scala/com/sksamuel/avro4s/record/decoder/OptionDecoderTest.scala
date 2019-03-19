package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import com.sksamuel.avro4s.Decoder._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, WordSpec}

case class OptionBoolean(b: Option[Boolean])
case class OptionString(s: Option[String])
case class StringClass(s: String)
case class OptionCaseClass(b: Option[StringClass])
case class NestedOptionCaseClass(child: Option[OptionCaseClass]=None)

case class NameV1(firstname: Option[String] = None)
case class NameV2(firstname: Option[String] = None, lastname: Option[String] = None)

case class PersonV1(name: Option[NameV1] = None)
case class PersonV2(name: Option[NameV2] = None)

class OptionDecoderTest extends WordSpec with Matchers {

  "Decoder" should {
    "support String options" in {
      val schema = AvroSchema[OptionString]

      val record1 = new GenericData.Record(schema)
      record1.put("s", "hello")
      Decoder[OptionString].decode(record1, schema) shouldBe OptionString(Some("hello"))

      val record2 = new GenericData.Record(schema)
      record2.put("s", null)
      Decoder[OptionString].decode(record2, schema) shouldBe OptionString(None)
    }
    "support boolean options" in {
      val schema = AvroSchema[OptionBoolean]

      val record1 = new GenericData.Record(schema)
      record1.put("b", true)
      Decoder[OptionBoolean].decode(record1, schema) shouldBe OptionBoolean(Some(true))

      val record2 = new GenericData.Record(schema)
      record2.put("b", null)
      Decoder[OptionBoolean].decode(record2, schema) shouldBe OptionBoolean(None)
    }
    "support options of case classes" in {
      val schema = AvroSchema[OptionCaseClass]
      val subSchema = AvroSchema[StringClass]

      val fooRecord1 = new GenericData.Record(subSchema)
      fooRecord1.put("s", new Utf8("hello"))
      val testRecord1 = new GenericData.Record(schema)
      testRecord1.put("b", fooRecord1)
      Decoder[OptionCaseClass].decode(testRecord1, schema) shouldBe OptionCaseClass(Some(StringClass("hello")))

      val testRecord2 = new GenericData.Record(schema)
      testRecord2.put("b", null)
      Decoder[OptionCaseClass].decode(testRecord2, schema) shouldBe OptionCaseClass(None)
    }
    "support options of nested case classes" in {
      val schema = AvroSchema[NestedOptionCaseClass]
      val optionSchema = AvroSchema[OptionCaseClass]
      val subSchema = AvroSchema[StringClass]

      val subRecord1 = new GenericData.Record(subSchema)
      subRecord1.put("s", new Utf8("hello"))
      val optionRecord1 = new GenericData.Record(optionSchema)
      optionRecord1.put("b", subRecord1)
      val rootRecord1 = new GenericData.Record(schema)
      rootRecord1.put("child", optionRecord1)
      Decoder[NestedOptionCaseClass].decode(rootRecord1, schema) shouldBe NestedOptionCaseClass(Some(OptionCaseClass(Some(StringClass("hello")))))

      val optionRecord2 = new GenericData.Record(optionSchema)
      optionRecord2.put("b", null)
      val rootRecord2 = new GenericData.Record(schema)
      rootRecord2.put("child", optionRecord2)
      Decoder[NestedOptionCaseClass].decode(rootRecord2, schema) shouldBe NestedOptionCaseClass(Some(OptionCaseClass(None)))

      val rootRecord3 = new GenericData.Record(schema)
      rootRecord3.put("child", null)
      Decoder[NestedOptionCaseClass].decode(rootRecord3, schema) shouldBe NestedOptionCaseClass(None)
    }
    "support reading old records with read compatible schema" in {
      val oldSchema = AvroSchema[NameV1]
      val newSchema = AvroSchema[NameV2]

      val oldRecord = new GenericData.Record(oldSchema)
      oldRecord.put("firstname", new Utf8("MyName"))
      Decoder[NameV2].decode(oldRecord, newSchema) shouldBe NameV2(Some("MyName"), None)
    }

    "support reading old records with nested case class and read compatible schema" in {
      val oldSchema = AvroSchema[PersonV1]
      val newSchema = AvroSchema[PersonV2]

      val oldNameSchema = AvroSchema[NameV1]

      val oldNameRecord = new GenericData.Record(oldNameSchema)
      oldNameRecord.put("firstname", new Utf8("MyName"))
      val oldPersonRecord = new GenericData.Record(oldSchema)
      oldPersonRecord.put("name", oldNameRecord)

      Decoder[PersonV2].decode(oldPersonRecord, newSchema) shouldBe PersonV2(Some(NameV2(Some("MyName"), None)))
    }
  }
}

