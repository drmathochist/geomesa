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

import java.io.Serializable
import java.{util => ju}

import org.geoserver.catalog.{Catalog, DataStoreInfo, WorkspaceInfo}
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.locationtech.geomesa.accumulo.csv
import org.locationtech.geomesa.process.ImportProcess
import org.locationtech.geomesa.web.csv.CSVUploadCache.{RecordTag, Record}

// CSV ingest approach in parallel with ImportProcess; can be moved to wpsanalytics?

trait CSVIngest {
  this: UserAuthentication with WorkspaceAccess with StoreAccess =>

  def csvUploadCache: CSVUploadCache
  def importer: ImportProcess

  @DescribeResult(name = "layerName", description = "Name of the new featuretype, with workspace")
  def execute(
               @DescribeParameter(
                 name = "csvId",
                 description = "The temporary ID of the CSV file to ingest")
               csvId: String,

               @DescribeParameter(
                 name = "keywords",
                 min = 0,
                 collectionType = classOf[String],
                 description = "List of (comma-separated) keywords for layer")
               keywordStrs: ju.List[String],

               @DescribeParameter(
                 name = "numShards",
                 min = 0,
                 max= 1,
                 description = "Number of shards to store for this table (defaults to 4)")
               numShards: Integer,

               @DescribeParameter(
                 name = "securityLevel",
                 min = 0,
                 max = 1,
                 description = "The level of security to apply to this import")
               securityLevel: String
              ) = {

    def ingest(userName: Option[String], record: Record) = {
      val fc       = csv.csvToFeatures(record.csvFile, record.hasHeader, record.schema)
      val name     = record.schema.name

      val workspace = getWorkspace(userName)
      val store = getStore(workspace)

      importer.execute(fc, workspace.getName, store.getName, name, keywordStrs, numShards, securityLevel)
    }

    val userName = getUserName
    val tag = RecordTag(userName, csvId)
    Option(csvUploadCache.load(tag)).map(ingest(userName, _))
                                    .getOrElse("")  // return an empty string for a failed ingest; better ideas?
  }
}

@DescribeProcess(
  title = "Ingest CSV data",
  description = "Ingest the data contained in an uploaded CSV file"
)
class IngestCSVProcess(val csvUploadCache: CSVUploadCache,
                       val importer: ImportProcess,
                       val catalog: Catalog,
                       val adsParams: ADSParams)
  extends CSVIngest with BasicUserAuthentication with WorkspaceAccess with StoreAccess

trait WorkspaceAccess {
  def catalog: Catalog

  def getWorkspace(userName: Option[String]) =
    lookupWorkspace(userName) match {
      case Some(ws) => ws
      case None     => buildWorkspace(userName)
    }

  def wsName(userName: Option[String]) =
    s"${userName.getOrElse("ANONYMOUS")}_LAYERS"

  def lookupWorkspace(userName: Option[String]) =
    Option(catalog.getWorkspaceByName(wsName(userName)))

  def buildWorkspace(userName: Option[String]) = {
    val ws = catalog.getFactory.createWorkspace()
    ws.setName(wsName(userName))
    catalog.add(ws)
    ws
  }
}

trait StoreAccess {

  def catalog: Catalog
  def adsParams: ADSParams

  def getStore(workspace: WorkspaceInfo) =
    lookupStore(workspace) match {
      case Some(s) => s
      case None    => buildStore(workspace)
    }

  val csvStoreName = "csvUploads"

  def lookupStore(workspace: WorkspaceInfo) =
    Option(catalog.getStoreByName(workspace, csvStoreName, classOf[DataStoreInfo]))

  def buildStore(workspace: WorkspaceInfo) = {
    val s = catalog.getFactory.createDataStore()
    s.setName(csvStoreName)
    s.setWorkspace(workspace)
    val params = s.getConnectionParameters
    params.putAll(adsParams.params)
    catalog.add(s)
    s
  }
}

case class ADSParams(instance: String,
                     zookeepers: String,
                     user: String,
                     password: String,
                     table: String) {
  import scala.collection.JavaConverters._

  def params: ju.Map[String, Serializable] =
    Map[String, Serializable]("instanceId" -> instance,
                              "zookeepers" -> zookeepers,
                              "user"       -> user,
                              "password"   -> password,
                              "tableName"  -> table
                             ).asJava
}
