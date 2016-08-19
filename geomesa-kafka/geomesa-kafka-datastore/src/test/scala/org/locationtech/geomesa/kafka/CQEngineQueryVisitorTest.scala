package org.locationtech.geomesa.kafka


import com.googlecode.cqengine.query.geo.Intersects
import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CQEngineQueryVisitorTest extends Specification {

  "Visitor" should {
    "translate filters" >> {

      val spec = "Who:String:index=full,What:Integer,When:Date,*Where:Point:srid=4326,Why:String"
      val sft = SimpleFeatureTypes.createType("test", spec)
      val cqAttributes = SFTAttributes(sft)


      val visitor = new CQEngineQueryVisitor(sft)

      val filter1 = ECQL.toFilter("INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0)))")
      val filter2 = ECQL.toFilter("Who IN('Addams', 'Bierce')")
      val filter3 = ECQL.toFilter("INTERSECTS(Where, POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))) AND Who IN('Addams', 'Bierce')")

      val filters = Seq(filter1, filter2, filter3)

      val queries = filters.map { _.accept(visitor, null) }

      val query1 = filter1.accept(visitor, null)

      val where = cqAttributes.lookup[Geometry]("Where")
      val value = WKTUtils.read("POLYGON((0 0, 0 90, 180 90, 180 0, 0 0)))")

      val expectQuery1 = new Intersects(where, value)

      query1 must equalTo(expectQuery1)

//      true must equalTo(true)
    }
  }

}
