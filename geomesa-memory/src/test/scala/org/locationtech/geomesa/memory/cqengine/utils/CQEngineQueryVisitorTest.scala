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

  "Visitor" should {
    "basic queries" should {
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

    "queries should return the same number of results" >> {
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

      val testFilters = Seq(
        "Who IN('Addams', 'Bierce')",
        "What = 5",
        "What > 3",
//        "What >= 3",
        "INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0)))",
        "INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))) AND Who IN('Addams', 'Bierce')"
      )

      def checkFilter(filter: Filter, coll: IndexedCollection[SimpleFeature]): MatchResult[Int] = {
        val gtCount = getGeoToolsCount(filter)
        val cqCount = getCQEngineCount(filter, coll)

        println(s"GT: $gtCount CQ: $cqCount Filter: $filter")

        gtCount must equalTo(cqCount)
      }

      examplesBlock {
        for (i <- testFilters.indices) {
          val t = testFilters(i)
          s"for filter $t for a geo-only index" in {
            checkFilter(t, cqcache)
          }
          s"for filter $t for various indices" in {
            checkFilter(t, cqcache2)
          }
        }
      }
    }
  }
}

case class QueryTest(filter: Filter, expectedQuery: Query[SimpleFeature])
