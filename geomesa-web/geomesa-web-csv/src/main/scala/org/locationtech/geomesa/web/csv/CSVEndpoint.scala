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
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.FilenameUtils
import org.locationtech.geomesa.accumulo.{TypeSchema, csv}
import org.locationtech.geomesa.web.core.GeoMesaScalatraServlet
import org.locationtech.geomesa.web.scalatra.PkiAuthenticationSupport
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

// TODO:
// Right now we cannot have GeoServer directly access a secured .gml endpoint
// since the geoserver cert -- if it even tries to use one -- will not match
// the cert of the uploading user.  ahulbert has suggested using wps instead
// if using a servlet at all, which isn't a bad idea.
//
// There should be two wps processes
// 1) geomesa:csv2xsd infers a schema from the uploaded csv data;
//    the end user can just pass the head of their csv to minimize transfers
// 2) geomesa:csvimport takes a schema as well as the csv data and converts
//    csv records to SimpleFeatures on the fly for ingest
class CSVEndpoint(csvUploadCache: CSVUploadCache)
  extends GeoMesaScalatraServlet
          with FileUploadSupport
          with Logging
          with PkiAuthenticationSupport {

  override val root: String = "csv"

  // caps CSV file size at 10MB
  configureMultipartHandling(MultipartConfig(maxFileSize = Some(10*1024*1024)))
  error {
    case e: SizeConstraintExceededException => RequestEntityTooLarge("Uploaded file too large!")
  }

  import CSVUploadCache._

  private[this] def getUserName  = scentry.authenticate("Pki").map(_.dn)
  private[this] def getRecordTag = RecordTag(getUserName, params("csvid"))

  post("/") {
    try {
      val fileItem = fileParams("csvfile")
      val csvFile = File.createTempFile(FilenameUtils.removeExtension(fileItem.name), ".csv")
      fileItem.write(csvFile)
      val hasHeader = params.get("hasHeader").map(_.toBoolean).getOrElse(true)
      val schema = csv.guessTypes(csvFile, hasHeader)
      val csvId = UUID.randomUUID.toString
      csvUploadCache.store(RecordTag(getUserName, csvId),
                           Record(csvFile, hasHeader, schema))
      Ok(csvId)
    } catch {
      case ex: Throwable =>
        logger.warn("Error uploading CSV", ex)
        NotAcceptable(reason = ex.getMessage)
    }
  }

  get("/types/:csvid") {
    val tag = getRecordTag
    val record = csvUploadCache.load(tag)
    if (record == null) {
      NotFound()
    } else {
      val TypeSchema(name, schema, _) = record.schema
      Ok(s"$name\n$schema")
    }
  }

  post("/types/update/:csvid") {
    val tag = getRecordTag
    val record = csvUploadCache.load(tag)
    if (record == null) {
      BadRequest(reason = s"Could not find record ${tag.csvId} for user ${tag.userId}")
    } else {
      val name = params.getOrElse("name", record.schema.name)
      val schema = params.getOrElse("schema", record.schema.schema)
      val latLon = for (latf <- params.get("latField"); lonf <- params.get("lonField")) yield (latf, lonf)
      csvUploadCache.update(tag, record.copy(schema = TypeSchema(name, schema, latLon)))
      Ok()
    }
  }

  post("/delete/:csvid.csv") {
    val tag = getRecordTag
    csvUploadCache.clear(tag)
    Ok()
  }

  delete("/:csvid.csv") {
    val tag = getRecordTag
    csvUploadCache.clear(tag)
    Ok()
  }
}
