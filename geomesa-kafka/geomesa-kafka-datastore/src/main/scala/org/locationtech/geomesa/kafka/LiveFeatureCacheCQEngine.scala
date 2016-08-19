/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.UUID

import com.googlecode.cqengine.attribute.{Attribute, SimpleAttribute}
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.{Query, QueryFactory}
import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class LiveFeatureCacheCQEngine(val sft: SimpleFeatureType)
  extends LiveFeatureCache with LazyLogging {

  val ID: Attribute[SimpleFeature, String] = new SimpleAttribute[SimpleFeature, String]("ID") {
    override def getValue(sf: SimpleFeature, queryOptions: QueryOptions): String = {
      sf.getID
    }
  }
  val attrs = SFTAttributes(sft)

  val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()
  cqcache.addIndex(HashIndex.onAttribute(ID))
  /*
  cqcache.addIndex(HashIndex.onAttribute(WHO_ATTR))
  cqcache.addIndex(NavigableIndex.onAttribute(WHAT_ATTR))
  cqcache.addIndex(GeoIndex.onAttribute(whereSimpleAttribute))
  */

  override def cleanUp(): Unit = ???

  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val queryId = QueryFactory.equal(ID, sf.getID)
    val res = cqcache.retrieve(queryId)
    if (res.size > 0) {
      for (sf <- res.iterator) {
        cqcache.remove(sf)
      }
    }
    cqcache.add(sf)
  }

  override def getFeatureById(id: String): FeatureHolder = ???
  /*
    val queryId = QueryFactory.equal(ID, id)
    val res = cqcache.retrieve(queryId)
    res.iterator().headOption
    if (res.size > 0) {
      res.iterator.toList.headOption
    }
  }
  {
    val queryId = QueryFactory.equal(ID, id)
    val res = cqcache.retrieve(queryId)

  }*/

  override def removeFeature(toDelete: Delete): Unit = {
    val queryId = QueryFactory.equal(ID, toDelete.id)
    val res = cqcache.retrieve(queryId)
    for (sf <- res.iterator) cqcache.remove(sf)
  }

  override def clear(): Unit = ???

  override def size(): Int = ???

  override def size(filter: Filter): Int = ???

  override def getReaderForFilter(filter: Filter): FR = ???

  def getReaderForQuery(query: Query[SimpleFeature]): FR = {
    new DFR(sft, new DFI(cqcache.retrieve(query).iterator))
  }
}

case class SFTAttributes(sft: SimpleFeatureType) {
  private val attributes = sft.getAttributeDescriptors

  private val lookupMap: Map[String, Attribute[SimpleFeature, _]] = attributes.map { attr =>
    val name = attr.getLocalName
    name -> buildSimpleFeatureAttribute(attr.getType.getBinding, name)
  }.toMap

  // TODO: this is really, really bad :)
  def lookup[T](attributeName: String): Attribute[SimpleFeature, T] = {
    lookupMap(attributeName).asInstanceOf[Attribute[SimpleFeature, T]]
  }

  def buildSimpleFeatureAttribute[A](binding: Class[_], name: String): Attribute[SimpleFeature, _] = {
    binding match {
      case c if classOf[java.lang.String].isAssignableFrom(c) => new SimpleFeatureAttribute[String](name)
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => new SimpleFeatureAttribute[Integer](name)
      case c if classOf[java.lang.Long].isAssignableFrom(c) => new SimpleFeatureAttribute[java.lang.Long](name)
      case c if classOf[java.lang.Float].isAssignableFrom(c) => new SimpleFeatureAttribute[java.lang.Float](name)
      case c if classOf[java.lang.Double].isAssignableFrom(c) => new SimpleFeatureAttribute[java.lang.Double](name)
      case c if classOf[java.lang.Boolean].isAssignableFrom(c) => new SimpleFeatureAttribute[java.lang.Boolean](name)
      case c if classOf[java.util.Date].isAssignableFrom(c) => new SimpleFeatureAttribute[java.util.Date](name)
      case c if classOf[UUID].isAssignableFrom(c) => new SimpleFeatureAttribute[UUID](name)
      case c if classOf[Geometry].isAssignableFrom(c) => new SimpleFeatureAttribute[Geometry](name)
    }
  }
}

// TODO: optimize by using field number rather than name.
class SimpleFeatureAttribute[A](name: String)(implicit ct: ClassTag[A]) extends
  SimpleAttribute[SimpleFeature, A](classOf[SimpleFeature], ct.runtimeClass.asInstanceOf[Class[A]], name) {
  override def getValue(feature: SimpleFeature, queryOptions: QueryOptions): A = {
    feature.getAttribute(name).asInstanceOf[A]
  }
}

