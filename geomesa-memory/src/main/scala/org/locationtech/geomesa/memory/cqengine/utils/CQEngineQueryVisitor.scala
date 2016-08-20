package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.attribute.Attribute
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

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new Exception(s"Filter visitor didn't recognize filter: $f.")
      }
    }.toList
    new com.googlecode.cqengine.query.logical.And[SimpleFeature](query)
  }

  override def visit(filter: Or, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new Exception(s"Filter visitor didn't recognize filter: $f.")
      }
    }.toList
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

  override def visit(filter: PropertyIsEqualTo, data: scala.Any): AnyRef = {
    val (attribute: Attribute[SimpleFeature, Any], value: Any) = extractAttributeAndValue(filter)
    new com.googlecode.cqengine.query.simple.Equal(attribute, value)
  }

  def extractAttributeAndValue(filter: Filter): (Attribute[SimpleFeature, Any], Any) = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    val attribute = lookup.lookup[Any](attributeName)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    (attribute, value)
  }

  override def toString = s"CQEngineQueryVisit()"

  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): AnyRef = {
    val (attribute: Attribute[SimpleFeature, Integer], value: Integer) = extractIntegerAttributeAndValue(filter)
    new com.googlecode.cqengine.query.simple.GreaterThan(attribute, value, false)
  }

  // Dealing with the comparable business is tough:(
  def extractIntegerAttributeAndValue(filter: Filter): (Attribute[SimpleFeature, Integer], Integer) = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name

    val attribute = lookup.lookupComparable[Integer](attributeName)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    (attribute, value)
  }
}