/*
 * Copyright 2017 Hudson's Bay Company
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gilt.spark.avro

import java.io.ByteArrayOutputStream
import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import scala.util.Try

trait AvroCustomTypes {

  def getFixedType(schema:Schema): DataType = {
    if (fieldOfUuidType(schema.getFullName)) {
      StringType
    }
    else {
      BinaryType
    }
  }

  def decodeUuid(anything: Any): Try[String] = Try {
    if(anything != null) {
      def failWithErrorMessage = {
        throw new Exception(s"Expected GenericFixed(16) object, got " +
          s"${anything.getClass.getName} :: ${anything}")
      }
      anything match {
        case x: GenericFixed =>
          if (x.bytes.length == 16) {
            decodeBytes(x.bytes).toString
          } else {
            failWithErrorMessage
          }
        case _ =>
          failWithErrorMessage
      }
    }
    else {
      null
    }
  }

  /** Converts UUID to avro bytes. */
  def encodeUuid( uuid: UUID
                ): GenericData.Fixed = {

    new Fixed(uuidAvroSchema, encodeBytes(uuid))
  }

  /** Converts UUID to avro bytes. */
  def encodeBytes( uuid: UUID
                 ): Array[Byte] = {

    val bos = new ByteArrayOutputStream(16)

    def writeLong(l: Long) {
      for ( i <- 7 to 0 by -1 ) {
        bos.write((l >> (i*8) & 0xFF).toInt)
      }
    }

    writeLong(uuid.getMostSignificantBits)
    writeLong(uuid.getLeastSignificantBits)
    bos.toByteArray
  }

  final private val _gfc_avro_UUID = "gfc.avro.UUID"
  final private val _java_util_UUID = "java.util.UUID"
  // final private val _gfc_avro_DateTime = "gfc.avro.DateTime"
  // final private val _gfc_avro_BigDecimal = "gfc.avro.BigDecimal"

  private
  val uuidAvroSchema = { // a lot depends on it, don't expect this schema to change
    val schemaJSON = """{"type":"fixed","name":"UUID","namespace":"gfc.avro","size":16}"""
    (new Schema.Parser()).parse(schemaJSON)
  }

  private
  def fieldOfUuidType(fullname: String): Boolean =
    fullname == _gfc_avro_UUID || fullname == _java_util_UUID

  private
  def decodeBytes(bs: Array[Byte]): UUID = {
    def decodeLong(offset: Int) = {
      var l = 0L
      for ( i <- 0 to 7 ) {
        l = (l << 8) | (bs(offset + i) & 0xFF)
      }
      l
    }
    new UUID(decodeLong(0), decodeLong(8))
  }
}
