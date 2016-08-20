package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.query.{Query, QueryFactory => CQF}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.visitor.AbstractFilterVisitor
import org.locationtech.geomesa.memory.cqengine.query.{Intersects => CQIntersects}
import org.locationtech.geomesa.filter._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._

import scala.collection.JavaConversions._
import scala.language._

class CQEngineQueryVisitor(sft: SimpleFeatureType) extends AbstractFilterVisitor {
  val lookup = SFTAttributes(sft)

  override def visit(filter: And, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map(_.accept(this, null).asInstanceOf[Query[SimpleFeature]]).toList
    new com.googlecode.cqengine.query.logical.And[SimpleFeature](query)
  }

  override def visit(filter: Or, data: scala.Any): AnyRef = {
   val children = filter.getChildren

    val query = children.map(_.accept(this, null).asInstanceOf[Query[SimpleFeature]]).toList
    new com.googlecode.cqengine.query.logical.Or[SimpleFeature](query)
  }

  override def visit(filter: BBOX, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression1.asInstanceOf[PropertyName].getPropertyName
    val geom = filter.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])

    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
  }

  override def visit(filter: Intersects, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression1.asInstanceOf[PropertyName].getPropertyName
    val geom = filter.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])

    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
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