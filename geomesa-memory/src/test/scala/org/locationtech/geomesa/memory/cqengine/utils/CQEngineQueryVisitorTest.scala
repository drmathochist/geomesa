package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.query.{Query, QueryFactory => QF}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.query.Intersects
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CQEngineQueryVisitorTest extends Specification {
  val spec = "Who:String:index=full,What:Integer,When:Date,*Where:Point:srid=4326,Why:String"
  val sft = SimpleFeatureTypes.createType("test", spec)
  val cq = SFTAttributes(sft)
  val ff = CommonFactoryFinder.getFilterFactory2

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
  }
}

case class QueryTest(filter: Filter, expectedQuery: Query[SimpleFeature])
