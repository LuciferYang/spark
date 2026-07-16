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

import java.net.URI

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, FakeV2SessionCatalog, NoSuchNamespaceException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog => V1InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogManagerSuite extends SparkFunSuite with SQLHelper {

  private def createSessionCatalog(): SessionCatalog = {
    val catalog = new V1InMemoryCatalog()
    catalog.createDatabase(
      CatalogDatabase(SessionCatalog.DEFAULT_DATABASE, "", new URI("fake"), Map.empty),
      ignoreIfExists = true)
    new SessionCatalog(catalog, EmptyFunctionRegistry)
  }

  test("CatalogManager should reflect the changes of default catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
    assert(catalogManager.currentNamespace.sameElements(Array("default")))

    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName,
      SQLConf.DEFAULT_CATALOG.key -> "dummy") {
      // The current catalog should be changed if the default catalog is set.
      assert(catalogManager.currentCatalog.name() == "dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
    }
  }

  test("CatalogManager should keep the current catalog once set") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentCatalog.name() == "dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))

      withSQLConf("spark.sql.catalog.dummy2" -> classOf[DummyCatalog].getName,
        SQLConf.DEFAULT_CATALOG.key -> "dummy2") {
        // The current catalog shouldn't be changed if it's set before.
        assert(catalogManager.currentCatalog.name() == "dummy")
      }
    }
  }

  test("current namespace should be updated when switching current catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
      catalogManager.setCurrentNamespace(Array("a"))
      assert(catalogManager.currentNamespace.sameElements(Array("a")))

      // If we set current catalog to the same catalog, current namespace should stay the same.
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a")))

      // If we switch to a different catalog, current namespace should be reset.
      withSQLConf("spark.sql.catalog.dummy2" -> classOf[DummyCatalog].getName) {
        catalogManager.setCurrentCatalog("dummy2")
        assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
      }
    }
  }

  test("set current namespace") {
    val v1SessionCatalog = createSessionCatalog()
    v1SessionCatalog.createDatabase(
      CatalogDatabase(
        "test", "", v1SessionCatalog.getDefaultDBPath("test"), Map.empty),
      ignoreIfExists = false)
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, v1SessionCatalog)

    // If the current catalog is session catalog, setting current namespace actually sets
    // `SessionCatalog.currentDb`.
    catalogManager.setCurrentNamespace(Array("test"))
    assert(catalogManager.currentNamespace.sameElements(Array("test")))
    assert(v1SessionCatalog.getCurrentDatabase == "test")

    intercept[NoSuchNamespaceException] {
      catalogManager.setCurrentNamespace(Array("ns1", "ns2"))
    }

    // when switching current catalog, `SessionCatalog.currentDb` should be reset.
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(v1SessionCatalog.getCurrentDatabase == "default")
      catalogManager.setCurrentNamespace(Array("test2"))
      assert(v1SessionCatalog.getCurrentDatabase == "default")

      // Check namespace existence if currentCatalog implements SupportsNamespaces.
      withSQLConf("spark.sql.catalog.testCatalog" -> classOf[InMemoryTableCatalog].getName) {
        catalogManager.setCurrentCatalog("testCatalog")
        catalogManager.currentCatalog.asInstanceOf[InMemoryTableCatalog]
          .createNamespace(Array("test3"), Map.empty[String, String].asJava)
        assert(v1SessionCatalog.getCurrentDatabase == "default")
        catalogManager.setCurrentNamespace(Array("test3"))
        assert(v1SessionCatalog.getCurrentDatabase == "default")

        intercept[NoSuchNamespaceException] {
          catalogManager.setCurrentNamespace(Array("ns1", "ns2"))
        }
      }
    }
  }

  test("session catalog alias resolves to the session catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf(SQLConf.SESSION_CATALOG_ALIAS.key -> "spark_alias_catalog") {
      val viaAlias = catalogManager.catalog("spark_alias_catalog")
      assert(viaAlias.name() == CatalogManager.SESSION_CATALOG_NAME)
      // The alias resolves to the very same instance as the canonical session catalog name.
      assert(viaAlias eq catalogManager.catalog(CatalogManager.SESSION_CATALOG_NAME))
      // Alias comparison is case-insensitive, matching the canonical name handling.
      assert(catalogManager.catalog("SPARK_ALIAS_CATALOG").name() ==
        CatalogManager.SESSION_CATALOG_NAME)
      assert(catalogManager.isCatalogRegistered("spark_alias_catalog"))
    }
  }

  test("session catalog alias works as current/default catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf(SQLConf.SESSION_CATALOG_ALIAS.key -> "spark_alias_catalog") {
      catalogManager.setCurrentCatalog("spark_alias_catalog")
      assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)

      withSQLConf(SQLConf.DEFAULT_CATALOG.key -> "spark_alias_catalog") {
        val freshManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
        assert(freshManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
      }
    }
  }

  test("setCurrentCatalog with the session catalog alias is a noop and keeps the namespace") {
    val v1SessionCatalog = createSessionCatalog()
    v1SessionCatalog.createDatabase(
      CatalogDatabase("test", "", v1SessionCatalog.getDefaultDBPath("test"), Map.empty),
      ignoreIfExists = false)
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, v1SessionCatalog)
    withSQLConf(SQLConf.SESSION_CATALOG_ALIAS.key -> "spark_alias_catalog") {
      catalogManager.setCurrentNamespace(Array("test"))
      assert(catalogManager.currentNamespace.sameElements(Array("test")))
      // Switching to the alias while already on the (canonical) session catalog must be a noop,
      // i.e. the current namespace must NOT be reset. Before the normalization fix, the noop
      // guard was defeated by the alias and this reset the namespace to "default".
      catalogManager.setCurrentCatalog("spark_alias_catalog")
      assert(catalogManager.currentNamespace.sameElements(Array("test")))
    }
  }

  test("setCurrentCatalog stores the canonical name so it survives the alias being unset") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf(
      "spark.sql.catalog.other" -> classOf[DummyCatalog].getName,
      SQLConf.SESSION_CATALOG_ALIAS.key -> "spark_alias_catalog") {
      // Start on a non-session catalog so that switching to the alias actually runs the "switch"
      // branch of setCurrentCatalog (the guard is true), exercising the storage of the
      // normalized name into `_currentCatalogName`.
      catalogManager.setCurrentCatalog("other")
      catalogManager.setCurrentCatalog("spark_alias_catalog")
    }
    // Both configs are now unset. Because `setCurrentCatalog` normalized and stored the canonical
    // name (not the raw alias string), resolving the current catalog must still work. If the raw
    // alias had been stored, `currentCatalog` would call `catalog(alias)` and throw
    // CatalogNotFoundException.
    assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
  }

  test("session catalog alias shadows a same-named registered catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf(
      "spark.sql.catalog.spark_alias_catalog" -> classOf[DummyCatalog].getName,
      SQLConf.SESSION_CATALOG_ALIAS.key -> "spark_alias_catalog") {
      // The alias branch is checked before loading a plugin, so the alias wins and resolves to
      // the session catalog rather than the registered DummyCatalog.
      assert(catalogManager.catalog("spark_alias_catalog").name() ==
        CatalogManager.SESSION_CATALOG_NAME)
    }
  }

  test("session catalog alias is not registered when unset") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    intercept[CatalogNotFoundException] {
      catalogManager.catalog("spark_alias_catalog")
    }
    assert(!catalogManager.isCatalogRegistered("spark_alias_catalog"))
  }

  test("session catalog alias equal to global temp database is ignored") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    withSQLConf(SQLConf.SESSION_CATALOG_ALIAS.key -> globalTempDB) {
      // A multi-part identifier whose head equals the global temp database is handled by a
      // dedicated branch before catalog resolution, so such an alias must be ignored here.
      intercept[CatalogNotFoundException] {
        catalogManager.catalog(globalTempDB)
      }
    }
  }

  test("an illegal session catalog alias is rejected when set") {
    // Dotted, blank, empty, and whitespace-padded aliases are all rejected at set time.
    Seq("a.b", " ", "", " spark_alias_catalog ").foreach { illegal =>
      intercept[IllegalArgumentException] {
        withSQLConf(SQLConf.SESSION_CATALOG_ALIAS.key -> illegal) {}
      }
    }
  }
}

class DummyCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
  }
  private var _name: String = null
  override def name(): String = _name
  override def defaultNamespace(): Array[String] = Array("a", "b")
}
