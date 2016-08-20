package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.query.{Query, QueryFactory => QF}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.query.Intersects
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import SampleFeatures._
import org.specs2.matcher.MatchResult

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
      val defaultGeom: Attribute[SimpleFeature, Geometry] =
        new SimpleFeatureAttribute(classOf[Geometry], sft.getGeometryDescriptor.getLocalName)

      val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()
      cqcache.addIndex(GeoIndex.onAttribute(defaultGeom))
      cqcache.addAll(feats)

      def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)
      def getCQEngineCount(filter: Filter) = {
        val visitor = new CQEngineQueryVisitor(sft)
        val query: Query[SimpleFeature] = filter.accept(visitor, null).asInstanceOf[Query[SimpleFeature]]
        println(s"Query for CQCache: $query")
        cqcache.retrieve(query).iterator().toList.size
      }

      val testFilters = Seq(
        "Who IN('Addams', 'Bierce')",
        "What = 5",
        "What > 3",
        "What >= 3"
      )

      def checkFilter(filter: Filter): MatchResult[Int] = {
        val gtCount = getGeoToolsCount(filter)
        val cqCount = getCQEngineCount(filter)

        println(s"GT: $gtCount CQ: $cqCount Filter: $filter")

        gtCount must equalTo(cqCount)
      }

      examplesBlock {
        for (i <- testFilters.indices) {
          "query_correctness" + i.toString in {
            val t = testFilters(i)
            checkFilter(t)
          }
        }
      }
    }
  }
}

case class QueryTest(filter: Filter, expectedQuery: Query[SimpleFeature])
