/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.IndexedCollection
import com.googlecode.cqengine.query.{Query, QueryFactory => QF}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.query.Intersects
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CQEngineQueryVisitorTest extends Specification {
  implicit def stringToFilter(s: String): Filter = ECQL.toFilter(s)

  "CQEngineQueryVisitor" should {
    "parse filters" should {
      sequential

      val visitor = new CQEngineQueryVisitor(sft)

      val whoAttr = cq.lookup[String]("Who")
      val whereAttr = cq.lookup[Geometry]("Where")

      val testFilters: Seq[QueryTest] = Seq(
        QueryTest(
          ECQL.toFilter("BBOX(Where, 0, 0, 180, 90)"),
          new Intersects(whereAttr, WKTUtils.read("POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))"))
        ),
        QueryTest(
          ECQL.toFilter("INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0)))"),
          new Intersects(whereAttr, WKTUtils.read("POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))"))
        ),
        QueryTest(
          ECQL.toFilter("Who IN('Addams', 'Bierce')"),
          QF.or(
            QF.equal[SimpleFeature, String](whoAttr, "Addams"),
            QF.equal[SimpleFeature, String](whoAttr, "Bierce"))
        ),
        QueryTest(
          ECQL.toFilter("INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))) AND Who IN('Addams', 'Bierce')"),
          QF.and(
            new Intersects(whereAttr, WKTUtils.read("POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))")),
            QF.or(
              QF.equal[SimpleFeature, String](whoAttr, "Addams"),
              QF.equal[SimpleFeature, String](whoAttr, "Bierce")))
        )
      )
      examplesBlock {
        for (i <- testFilters.indices) {
          "query_" + i.toString in {
            val t = testFilters(i)
            val query = t.filter.accept(visitor, null)

            query must equalTo(t.expectedQuery)
          }
        }
      }
    }

    "return correct number of results" >> {
      val feats = (0 until 1000).map(SampleFeatures.buildFeature)

      // Set up CQEngine with a Geo-index.
      val cqcache: IndexedCollection[SimpleFeature] = CQIndexingOptions.buildIndexedCollection(sft)
      cqcache.addAll(feats)

      val cqcache2: IndexedCollection[SimpleFeature] = CQIndexingOptions.buildIndexedCollection(sftWithIndexes)
      cqcache2.addAll(feats)

      def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)
      def getCQEngineCount(filter: Filter, coll: IndexedCollection[SimpleFeature]) = {
        val visitor = new CQEngineQueryVisitor(sft)
        val query: Query[SimpleFeature] = filter.accept(visitor, null).asInstanceOf[Query[SimpleFeature]]
        println(s"Query for CQCache: $query")
        coll.retrieve(query).iterator().toList.size
      }

      def checkFilter(filter: Filter, coll: IndexedCollection[SimpleFeature]): MatchResult[Int] = {
        val gtCount = getGeoToolsCount(filter)
        val cqCount = getCQEngineCount(filter, coll)

        println(s"GT: $gtCount CQ: $cqCount Filter: $filter")

        gtCount must equalTo(cqCount)
      }

      def runFilterTests(name: String, filters: Seq[Filter]) = {
        examplesBlock {
          for (f <- filters) {
            s"$name filter $f (geo-only index)" in {
              checkFilter(f, cqcache)
            }
            s"$name filter $f (various indices)" in {
              checkFilter(f, cqcache2)
            }
          }
        }
      }

      // big enough so there are likely to be points in them
      val bbox1 = "POLYGON((-89 89, -1 89, -1 -89, -89 -89, -89 89))"
      val bbox2 = "POLYGON((-180 90, 0 90, 0 0, -180 0, -180 90))"

      val basicFilters = Seq[Filter](
        "Who IN('Addams', 'Bierce')",
        "What = 5",
        s"INTERSECTS(Where, $bbox1)",
        s"INTERSECTS(Where, $bbox2) AND Who IN('Addams', 'Bierce')",
        s"NOT (INTERSECTS(Where, $bbox1))",
        s"NOT (INTERSECTS(Where, $bbox1))",
        "When BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'",
        "When BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'"
      )
      runFilterTests("basic", basicFilters)

      val comparableFilters = Seq[Filter](
        "What = 5",
        "WhatLong = 5",

        "What > 5",
        "WhatLong > 5",
        "WhatFloat > 5.0",
        "WhatDouble > 5.0",

        "What >= 5",
        "WhatLong >= 5",
        "WhatFloat >= 5.0",
        "WhatDouble >= 5.0",

        "What < 5",
        "WhatLong < 5",
        "WhatFloat < 5.0",
        "WhatDouble < 5.0",

        "What <= 5",
        "WhatLong <= 5",
        "WhatFloat <= 5.0",
        "WhatDouble <= 5.0"
      )
      runFilterTests("comparable", comparableFilters)

      val oneLevelAndFilters: Seq[Filter] = Seq(
        s"(INTERSECTS(Where, $bbox1) AND INTERSECTS(Where, $bbox2))",
        s"(INTERSECTS(Where, $bbox1) AND Who = 'Addams')",
        s"(Who = 'Addams' AND INTERSECTS(Where, $bbox1))",
        s"INTERSECTS(Where, $bbox1) AND When DURING 2010-08-08T00:00:00.000Z/2010-08-08T23:59:59.000Z"
      )
      runFilterTests("one level AND", oneLevelAndFilters)
    }
  }
}

case class QueryTest(filter: Filter, expectedQuery: Query[SimpleFeature])
