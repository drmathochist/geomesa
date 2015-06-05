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

import org.geoserver.security.xml.XMLGeoserverUser
import org.locationtech.geomesa.process.GeomesaProcess
import org.locationtech.geomesa.web.csv.CSVUploadCache.RecordTag
import org.springframework.security.core.context.SecurityContextHolder

class GeomesaCSVProcess(csvUploadCache: CSVUploadCache) extends GeomesaProcess {

  def getUserName: Option[String] = {
    for {
      auth <- Option(SecurityContextHolder.getContext.getAuthentication)
    } yield {
      auth.getPrincipal match {
        case xml: XMLGeoserverUser => xml.getUsername
        case str: String           => str.toLowerCase
      }
    }
  }
  def getTag(csvId: String): RecordTag = RecordTag(getUserName, csvId)
}
