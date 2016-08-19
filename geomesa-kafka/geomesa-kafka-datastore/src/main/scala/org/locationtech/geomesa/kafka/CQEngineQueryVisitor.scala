package org.locationtech.geomesa.kafka


import com.google.common.base.Ticker
import com.googlecode.cqengine.index.geo.GeoIndex
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.query.{QueryFactory => CQF}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.{AbstractFilterVisitor, SimplifyingFilterVisitor}
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language._
import scala.util.Random

class CQEngineQueryVisitor(sft: SimpleFeatureType) extends AbstractFilterVisitor {
  val lookup = SFTAttributes(sft)

  override def visit(filter: And, data: scala.Any): AnyRef = {
    //super.visit(filter, data)

    val children = filter.getChildren

    val query = children.map(_.accept(this, null).asInstanceOf[Query[SimpleFeature]]).toList
    new com.googlecode.cqengine.query.logical.And[SimpleFeature](query)
  }

  override def visit(filter: Or, data: scala.Any): AnyRef = {
    //super.visit(filter, data)

    val children = filter.getChildren

    val query = children.map(_.accept(this, null).asInstanceOf[Query[SimpleFeature]]).toList
    new com.googlecode.cqengine.query.logical.Or[SimpleFeature](query)
  }

  override def visit(filter: Intersects, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression1.asInstanceOf[PropertyName].getPropertyName
    val geom = filter.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])

    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new com.googlecode.cqengine.query.geo.Intersects(geomAttribute, geom)
  }

  // JNH: This is tricky
  override def visit(filter: PropertyIsEqualTo, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get

    val attributeName = prop.name
    val value = prop.literal.evaluate(null, classOf[Any])

    val equalAttribute = lookup.lookup[Any](attributeName)

    // The two Any's likely blows this up.
    new com.googlecode.cqengine.query.simple.Equal(equalAttribute, value)
  }

  override def toString = s"CQEngineQueryVisit()"
}