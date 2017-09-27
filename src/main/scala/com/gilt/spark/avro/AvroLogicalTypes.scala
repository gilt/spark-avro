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


import org.apache.avro.data.TimeConversions.TimestampConversion
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.avro.{Conversions, LogicalType, LogicalTypes, Schema}

import scala.util.Try

trait AvroLogicalTypes {

  def getBytesType(schema:Schema): DataType = {
    schema.getLogicalType match {
      case null => BinaryType
      case l =>
        Some(l).get match {
          case d:LogicalTypes.Decimal =>
            DecimalType(d.getPrecision,d.getScale)
          case _ =>
            BinaryType
        }
      }
  }

  def getLongType(schema:Schema): DataType = {
    schema.getLogicalType match {
      case null => LongType
      case l => Some(l).get match {
        case d:LogicalTypes.TimestampMillis => TimestampType
        case _ => LongType
      }
    }
  }

  def decodeTimestamp(anything: Any,
                      schema: Schema): Try[Timestamp] = Try {
    if (anything != null) {
      val millis = anything.asInstanceOf[Long]
      getLogicalType(schema) match {
        case Some(logical) => logical match {
          case l: LogicalTypes.TimestampMillis =>
            if (millis == 0) {
              null
            }
            else {
              new Timestamp(millis)
            }
          case l =>
            throw new IllegalArgumentException(s"Unsupported Avro logical type " +
              s"${l.getName} for Avro TimestampMillis.")
        }
        case None =>
          throw new IllegalArgumentException(s"Missing Avro logical type " +
            s"for Avro TimestampMillis.")
      }
    }
    else {
      null
    }
  }

  def decodeDecimal(anything: Any,
                    schema: Schema): Try[java.math.BigDecimal] = Try {
    if (anything != null) {
      val decimalBytes = anything.asInstanceOf[ByteBuffer]
      getLogicalType(schema) match {
        case Some(logical) => logical match {
          case l: LogicalTypes.Decimal =>
            (new Conversions.DecimalConversion()).fromBytes(decimalBytes, schema, l)
          case l =>
            throw new IllegalArgumentException(s"Unsupported Avro logical type " +
              s"${l.getName} for Avro TimestampMillis.")
        }
        case None =>
          throw new IllegalArgumentException(s"Missing Avro logical type " +
            s"for Avro TimestampMillis.")
      }
    }
    else {
      null
    }
  }

  private
  def getLogicalType(schema:Schema): Option[LogicalType] = {
    schema.getLogicalType match {
      case null => None
      case l => Some(l)
    }
  }


}
