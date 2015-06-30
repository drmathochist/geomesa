package org.locationtech.geomesa.web.csv

import java.io.Serializable
import java.{util => ju}

import org.geoserver.catalog.{Catalog, DataStoreInfo, WorkspaceInfo}
import org.geoserver.security.AccessMode
import org.geoserver.security.impl.{DataAccessRule, DataAccessRuleDAO}
import org.geotools.process.factory.{DescribeResult, DescribeParameter, DescribeProcess}
import org.locationtech.geomesa.accumulo.csv
import org.locationtech.geomesa.plugin.security.UserNameRoles
import org.locationtech.geomesa.process.ImportProcess
import org.locationtech.geomesa.web.csv.CSVUploadCache.{RecordTag, Record}

@DescribeProcess(
  title = "Ingest CSV data",
  description = "Ingest the data contained in an uploaded CSV file"
)
class IngestCSVProcess(csvUploadCache: CSVUploadCache,
                       importer: ImportProcess,
                       catalog: Catalog,
                       dataAccessRuleDAO: DataAccessRuleDAO,
                       adsParams: ADSParams)
  extends GeomesaCSVProcess(csvUploadCache) {

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
    def wsName(userName: String) = s"${userName}_LAYERS"
    def getUserWorkSpace(userName: String) =
      Option(catalog.getWorkspaceByName(wsName(userName)))
    val csvStoreName = "csvUploads"
    def getCSVStore(workspace: WorkspaceInfo) =
      Option(catalog.getStoreByName(workspace, csvStoreName, classOf[DataStoreInfo]))

    def ingest(userNameO: Option[String], record: Record) = {
      val userName = userNameO.getOrElse("ANONYMOUS")
      val fc       = csv.csvToFeatures(record.csvFile, record.hasHeader, record.schema)
      val name     = record.schema.name

      val workspace = getUserWorkSpace(userName) match {
        case Some(ws) => ws
        case None     =>
          val ws = catalog.getFactory.createWorkspace()
          ws.setName(wsName(userName))
          catalog.add(ws)

          for (un <- userNameO) {
            val userRoleName = UserNameRoles.userRoleName(un)
            val readRule = new DataAccessRule(ws.getName, DataAccessRule.ANY, AccessMode.READ, userRoleName)
            dataAccessRuleDAO.addRule(readRule)
            val writeRule = new DataAccessRule(ws.getName, DataAccessRule.ANY, AccessMode.WRITE, userRoleName)
            dataAccessRuleDAO.addRule(writeRule)
          }

          ws  // still needs userspace locking!
      }

      val store = getCSVStore(workspace) match {
        case Some(s) => s
        case None    =>
          val s = catalog.getFactory.createDataStore()
          s.setName(csvStoreName)
          s.setWorkspace(workspace)
          val params = s.getConnectionParameters
          params.putAll(adsParams.params)
          catalog.add(s)
          s
      }
      importer.execute(fc, workspace.getName, store.getName, name, keywordStrs, numShards, securityLevel)
    }

    val userName = getUserAuth.map(getUserName)
    val tag = RecordTag(userName, csvId)
    Option(csvUploadCache.load(tag)).map(ingest(userName, _))
                                    .getOrElse("")  // return an empty string for a failed ingest; better ideas?
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