package org.locationtech.geomesa.memory.cqengine.utils

import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.googlecode.cqengine.attribute.Attribute
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.opengis.feature.`type`.AttributeDescriptor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try

// See geomesa/geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/geotools/Conversions.scala
object CQIndexingOptions {
  def getCQIndexType(ad: AttributeDescriptor): CQIndexType = {
    Option(ad.getUserData.get(OPT_CQ_INDEX).asInstanceOf[String])
      .flatMap(c => Try(CQIndexType.withName(c)).toOption).getOrElse(CQIndexType.NONE)
  }

  def setCQIndexType(ad: AttributeDescriptor, indexType: CQIndexType) {
    ad.getUserData.put(OPT_CQ_INDEX, indexType.toString)
  }

  def buildIndexedCollection(sft: SimpleFeatureType): IndexedCollection[SimpleFeature] = {
    val defaultGeom: Attribute[SimpleFeature, Geometry] =
      new SimpleFeatureAttribute(classOf[Geometry], sft.getGeometryDescriptor.getLocalName)

    // TODO: Add logic to allow for the geo-index to be disabled?
    val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()
    cqcache.addIndex(GeoIndex.onAttribute(defaultGeom))

    cqcache
  }
}

object CQIndexType extends Enumeration {
  type CQIndexType = Value
  val DEFAULT   = Value("default")   // Let GeoMesa pick.
  val NAVIGABLE = Value("navigable") // Use for numeric fields and Date?
  val RADIX     = Value("radix")     // Use for strings

  val UNIQUE    = Value("unique")    // Use only for unique fields; could be string, Int, Long
  val HASH      = Value("hash")      // Use for 'enumerated' strings

  val NONE      = Value("none")
}
