/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache._
import com.googlecode.cqengine.attribute.{Attribute, SimpleAttribute}
import com.googlecode.cqengine.index.geo.GeoIndex
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.{Query, QueryFactory}
import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.FilteringFeatureReader
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.filter.spatial.{BBOX, Intersects, Within}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

class LiveFeatureCacheCQEngine(sft: SimpleFeatureType,
                               expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends LiveFeatureCache with LazyLogging {

//  def this(sft: SimpleFeatureType) = {
//    this(sft, null)
//  }

  val defaultGeom: Attribute[SimpleFeature, Geometry] = new SimpleFeatureAttribute[Geometry](sft.getGeometryDescriptor.getLocalName)


  val attrs = SFTAttributes(sft)

  val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()
  cqcache.addIndex(GeoIndex.onAttribute(defaultGeom))

  // JNH: TODO: Add additional CQEngine indices ?


  // JNH: Do we need FeatureHolder anymore?
  private val cache: Cache[String, FeatureHolder] = {
    val cb = CacheBuilder.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            if (removal.getCause == RemovalCause.EXPIRED) {
              println(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")

              logger.debug(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")
              val ret = cqcache.remove(removal.getValue.sf)
              println(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms returned $ret from CQEngine")
              //spatialIndex.remove(removal.getValue.env, removal.getValue.sf)
            }
          }
        })
    }
    cb.build()
  }

  val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  def size(): Int = {
    cache.size().toInt // This returns an approximate per Guava's docs.
  }

  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      getReaderForFilter(f).getIterator.length
    }
  }

  /*
  cqcache.addIndex(HashIndex.onAttribute(WHO_ATTR))
  cqcache.addIndex(NavigableIndex.onAttribute(WHAT_ATTR))
  cqcache.addIndex(GeoIndex.onAttribute(whereSimpleAttribute))
  */

  override def cleanUp(): Unit = {}

  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = sf.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      cqcache.remove(old.sf)
    }
    val env = sf.geometry.getEnvelopeInternal
    cqcache.add(sf)
    cache.put(id, FeatureHolder(sf, env))
  }

  override def getFeatureById(id: String): FeatureHolder = cache.getIfPresent(id)

  override def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      //spatialIndex.remove(old.env, old.sf)
      cqcache.remove(old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    cqcache.clear()        // Consider re-instanting the CQCache
  }

//  override def size(filter: Filter): Int = ???

  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: IncludeFilter => include(f)
      case f: Id            => fid(f)
      case f                => queryCQ(f)
        // JNH: Consider testing filter rewrite before passing to CQEngine?
    }

  def include(i: IncludeFilter) = {
    println("Running Filter.INCLUDE")
    new DFR(sft, new DFI(features.valuesIterator.map(_.sf)))
  }

  def fid(ids: Id): FR = {
    println("Queried for IDs; using Guava ID index")
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
  }

  def queryCQ(f: Filter): FR = {
    val visitor = new CQEngineQueryVisitor(sft)

    val query = f.accept(visitor, null).asInstanceOf[Query[SimpleFeature]]
    println(s"Querying CQEngine with $query")
    new DFR(sft, new DFI(cqcache.retrieve(query).iterator()))
  }

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

