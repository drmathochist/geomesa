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

import org.geoserver.catalog.Catalog
import org.geoserver.security.AccessMode
import org.geoserver.security.impl.{DataAccessRule, DataAccessRuleDAO}
import org.geotools.process.factory.DescribeProcess
import org.locationtech.geomesa.plugin.security.UserNameRoles
import org.locationtech.geomesa.process.ImportProcess

// a version that can be moved into gs-ext along with

@DescribeProcess(
  title = "Ingest CSV data",
  description = "Ingest the data contained in an uploaded CSV file"
)
class UserLockingIngestCSVProcess(val csvUploadCache: CSVUploadCache,
                                  val importer: ImportProcess,
                                  val catalog: Catalog,
                                  val dataAccessRuleDAO: DataAccessRuleDAO,
                                  val adsParams: ADSParams)
  extends CSVIngest with BasicUserAuthentication with UserLockingWorkspaceAccess with StoreAccess

trait UserLockingWorkspaceAccess
  extends WorkspaceAccess {

  def dataAccessRuleDAO: DataAccessRuleDAO

  override def buildWorkspace(userName: Option[String]) = {
    val ws = super.buildWorkspace(userName)

    for (un <- userName) {
      val userRoleName = UserNameRoles.userRoleName(un)
      val readRule = new DataAccessRule(ws.getName, DataAccessRule.ANY, AccessMode.READ, userRoleName)
      dataAccessRuleDAO.addRule(readRule)
      val writeRule = new DataAccessRule(ws.getName, DataAccessRule.ANY, AccessMode.WRITE, userRoleName)
      dataAccessRuleDAO.addRule(writeRule)
    }

    ws
  }
}
