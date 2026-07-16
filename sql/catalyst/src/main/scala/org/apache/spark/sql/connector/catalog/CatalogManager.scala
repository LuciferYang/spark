/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog

import scala.collection.mutable

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.CATALOG_NAME
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, TempVariableManager}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * A thread-safe manager for [[CatalogPlugin]]s. It tracks all the registered catalogs, and allow
 * the caller to look up a catalog by name.
 *
 * There are still many commands (e.g. ANALYZE TABLE) that do not support v2 catalog API. They
 * ignore the current catalog and blindly go to the v1 `SessionCatalog`. To avoid tracking current
 * namespace in both `SessionCatalog` and `CatalogManger`, we let `CatalogManager` to set/get
 * current database of `SessionCatalog` when the current catalog is the session catalog.
 */
// TODO: all commands should look up table from the current catalog. The `SessionCatalog` doesn't
//       need to track current database at all.
private[sql]
class CatalogManager(
    defaultSessionCatalog: CatalogPlugin,
    val v1SessionCatalog: SessionCatalog) extends SQLConfHelper with Logging {
  import CatalogManager.SESSION_CATALOG_NAME
  import CatalogV2Util._

  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]

  // Warns at most once when the configured session catalog alias collides with the global temp
  // database name (in which case the alias is silently ignored, see `isSessionCatalogName`).
  private val aliasGlobalTempWarned = new java.util.concurrent.atomic.AtomicBoolean(false)

  // TODO: create a real SYSTEM catalog to host `TempVariableManager` under the SESSION namespace.
  val tempVariableManager: TempVariableManager = new TempVariableManager

  /**
   * Returns true if `name` refers to the built-in session catalog, either by its canonical name
   * `spark_catalog` or by the optional alias configured via `spark.sql.sessionCatalogAlias`.
   *
   * An alias that equals the global temp database name is ignored (a multi-part identifier whose
   * head equals the global temp database is handled by a dedicated branch in `LookupCatalog`
   * before catalog resolution, so such an alias would never take effect); we warn once so the
   * misconfiguration is visible instead of failing silently.
   */
  private def isSessionCatalogName(name: String): Boolean = {
    name.equalsIgnoreCase(SESSION_CATALOG_NAME) ||
      conf.getConf(SQLConf.SESSION_CATALOG_ALIAS).exists { alias =>
        if (alias.equalsIgnoreCase(conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE))) {
          if (aliasGlobalTempWarned.compareAndSet(false, true)) {
            logWarning(log"Session catalog alias '${MDC(CATALOG_NAME, alias)}' equals the " +
              log"global temp database name and is therefore ignored.")
          }
          false
        } else {
          alias.equalsIgnoreCase(name)
        }
      }
  }

  def catalog(name: String): CatalogPlugin = synchronized {
    if (isSessionCatalogName(name)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
    }
  }

  def isCatalogRegistered(name: String): Boolean = {
    try {
      catalog(name)
      true
    } catch {
      case _: CatalogNotFoundException => false
    }
  }

  private def loadV2SessionCatalog(): CatalogPlugin = {
    Catalogs.load(SESSION_CATALOG_NAME, conf) match {
      case extension: CatalogExtension =>
        extension.setDelegateCatalog(defaultSessionCatalog)
        extension
      case other => other
    }
  }

  /**
   * If the V2_SESSION_CATALOG config is specified, we try to instantiate the user-specified v2
   * session catalog. Otherwise, return the default session catalog.
   *
   * This catalog is a v2 catalog that delegates to the v1 session catalog. it is used when the
   * session catalog is responsible for an identifier, but the source requires the v2 catalog API.
   * This happens when the source implementation extends the v2 TableProvider API and is not listed
   * in the fallback configuration, spark.sql.sources.useV1SourceList
   */
  private[sql] def v2SessionCatalog: CatalogPlugin = {
    conf.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION) match {
      case "builtin" => defaultSessionCatalog
      case _ => catalogs.getOrElseUpdate(SESSION_CATALOG_NAME, loadV2SessionCatalog())
    }
  }

  private var _currentNamespace: Option[Array[String]] = None

  def currentNamespace: Array[String] = {
    val defaultNamespace = if (currentCatalog.name() == SESSION_CATALOG_NAME) {
      Array(v1SessionCatalog.getCurrentDatabase)
    } else {
      currentCatalog.defaultNamespace()
    }

    this.synchronized {
      _currentNamespace.getOrElse {
        defaultNamespace
      }
    }
  }

  private def assertNamespaceExist(namespace: Array[String]): Unit = {
    currentCatalog match {
      case catalog: SupportsNamespaces if !catalog.namespaceExists(namespace) =>
        throw QueryCompilationErrors.noSuchNamespaceError(catalog.name() +: namespace)
      case _ =>
    }
  }

  def setCurrentNamespace(namespace: Array[String]): Unit = synchronized {
    if (isSessionCatalog(currentCatalog) && namespace.length == 1) {
      v1SessionCatalog.setCurrentDatabaseWithNameCheck(
        namespace.head,
        _ => assertNamespaceExist(namespace))
    } else {
      assertNamespaceExist(namespace)
    }
    _currentNamespace = Some(namespace)
  }

  private var _currentCatalogName: Option[String] = None

  def currentCatalog: CatalogPlugin = synchronized {
    catalog(_currentCatalogName.getOrElse(conf.getConf(SQLConf.DEFAULT_CATALOG)))
  }

  def setCurrentCatalog(catalogName: String): Unit = synchronized {
    // Normalize an alias of the session catalog to its canonical name, so that the noop guard
    // below (which compares against `currentCatalog.name()`, always the canonical name) is not
    // defeated by the alias, and `_currentCatalogName` never stores an alias string that could
    // stop resolving if the alias config is later changed or unset.
    val normalizedName =
      if (isSessionCatalogName(catalogName)) SESSION_CATALOG_NAME else catalogName
    // `setCurrentCatalog` is noop if it doesn't switch to a different catalog.
    if (currentCatalog.name() != normalizedName) {
      catalog(normalizedName)
      _currentCatalogName = Some(normalizedName)
      _currentNamespace = None
      // Reset the current database of v1 `SessionCatalog` when switching current catalog, so that
      // when we switch back to session catalog, the current namespace definitely is ["default"].
      v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
    }
  }

  def listCatalogs(pattern: Option[String]): Seq[String] = {
    val allCatalogs = (synchronized(catalogs.keys.toSeq) :+ SESSION_CATALOG_NAME).distinct.sorted
    pattern.map(StringUtils.filterPattern(allCatalogs, _)).getOrElse(allCatalogs)
  }

  // Clear all the registered catalogs. Only used in tests.
  private[sql] def reset(): Unit = synchronized {
    catalogs.clear()
    _currentNamespace = None
    _currentCatalogName = None
    v1SessionCatalog.setCurrentDatabase(conf.defaultDatabase)
  }
}

private[sql] object CatalogManager {
  val SESSION_CATALOG_NAME: String = "spark_catalog"
  val SYSTEM_CATALOG_NAME = "system"
  val SESSION_NAMESPACE = "session"
}
