/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.web.csv

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.cache._
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.accumulo.TypeSchema

object CSVUploadCache {
  case class RecordTag(userId: Option[String], csvId: String)
  case class Record(csvFile: File, hasHeader: Boolean, schema: TypeSchema)
}

import CSVUploadCache._

class CSVUploadCache
  extends Logging {

  val records: Cache[RecordTag, Record] = {
    val removalListener = new RemovalListener[RecordTag, Record]() {
      override def onRemoval(notification: RemovalNotification[RecordTag, Record]) =
        if (notification.getCause != RemovalCause.REPLACED) {
          cleanup(notification.getKey, notification.getValue)
        }
    }
    CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .removalListener(removalListener)
      .build()
  }

  private def cleanup(tag: RecordTag, record: Record) {
    record.csvFile.delete()
    records.invalidate(tag)
  }

  private def userName(tag: RecordTag) = tag.userId.getOrElse("ANONYMOUS")
  def store(tag: RecordTag, record: Record) {
    Option(records.getIfPresent(tag)) match {
      case Some(_) =>
        logger.warn(s"User ${userName(tag)} has already stored a CSV with ID ${tag.csvId}; leaving it")
      case None    =>
        records.put(tag, record)
    }
  }
  def update(tag: RecordTag, record: Record) {
    Option(records.getIfPresent(tag)) match {
      case Some(oldRecord) =>
        if (oldRecord.csvFile != record.csvFile) oldRecord.csvFile.delete() // replacing entry will not delete old file
      case None            =>
        logger.warn(s"User ${userName(tag)} has not stored a CSV with ID ${tag.csvId}; storing instead")
    }
    records.put(tag, record)
  }
  def load(tag: RecordTag) = records.getIfPresent(tag)
  def clear(tag: RecordTag) { for {record <- Option(load(tag))} cleanup(tag, record) }
}
