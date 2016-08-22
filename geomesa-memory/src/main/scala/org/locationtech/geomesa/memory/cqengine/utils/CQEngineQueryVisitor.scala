/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.query.Query
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.visitor.AbstractFilterVisitor
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.query.{Intersects => CQIntersects}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

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

  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    sft.getDescriptor(attributeName).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Integer](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Integer])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Integer](attr, value, false)
      }
      case c if classOf[java.lang.Long].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Long](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Long])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Long](attr, value, false)
      }
      case c if classOf[java.lang.Float].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Float](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Float])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Float](attr, value, false)
      }
      case c if classOf[java.lang.Double].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Double](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Double])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Double](attr, value, false)
      }
    }
  }

  override def visit(filter: PropertyIsGreaterThanOrEqualTo, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    sft.getDescriptor(attributeName).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Integer](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Integer])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Integer](attr, value, true)
      }
      case c if classOf[java.lang.Long].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Long](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Long])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Long](attr, value, true)
      }
      case c if classOf[java.lang.Float].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Float](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Float])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Float](attr, value, true)
      }
      case c if classOf[java.lang.Double].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Double](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Double])
        new com.googlecode.cqengine.query.simple.GreaterThan[SimpleFeature, java.lang.Double](attr, value, true)
      }
    }
  }

  override def visit(filter: PropertyIsLessThan, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    sft.getDescriptor(attributeName).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Integer](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Integer])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Integer](attr, value, false)
      }
      case c if classOf[java.lang.Long].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Long](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Long])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Long](attr, value, false)
      }
      case c if classOf[java.lang.Float].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Float](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Float])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Float](attr, value, false)
      }
      case c if classOf[java.lang.Double].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Double](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Double])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Double](attr, value, false)
      }
    }
  }

  override def visit(filter: PropertyIsLessThanOrEqualTo, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    sft.getDescriptor(attributeName).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Integer](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Integer])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Integer](attr, value, true)
      }
      case c if classOf[java.lang.Long].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Long](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Long])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Long](attr, value, true)
      }
      case c if classOf[java.lang.Float].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Float](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Float])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Float](attr, value, true)
      }
      case c if classOf[java.lang.Double].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[java.lang.Double](attributeName)
        val value = prop.literal.evaluate(null, classOf[java.lang.Double])
        new com.googlecode.cqengine.query.simple.LessThan[SimpleFeature, java.lang.Double](attr, value, true)
      }
    }
  }

  def extractAttributeAndValue(filter: Filter): (Attribute[SimpleFeature, Any], Any) = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    val attribute = lookup.lookup[Any](attributeName)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    (attribute, value)
  }

  // JNH: TODO: revisit if this this needed.
  override def toString = "CQEngineQueryVisit()"

  // Dealing with the comparable business is tough:(
  /*
  def extractComparableAttributeAndValue[T](filter: Filter): (Attribute[SimpleFeature, T], T) = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name

    val attribute = lookup.lookupComparable[T](attributeName)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    (attribute, value)
  }
  */
}