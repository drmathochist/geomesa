/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

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
import CQIndexingOptions._

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CQIndexingOptionsTest extends Specification {

  "CQ Indexing options" should {
    "be configurable from SimpleFeatureTypes" >> {
      val spec = "Who:String:cq_index=default," +
        "What:Integer:cq_index=unique," +
        "When:Date:cq_index=navigable," +
        "*Where:Point:srid=4326," +
        "Why:String:cq_index=hash"

      val sft = SimpleFeatureTypes.createType("test", spec)

      "via SFT spec" >> {
        val whoDescriptor = sft.getDescriptor("Who")
        getCQIndexType(whoDescriptor) mustEqual CQIndexType.DEFAULT

        val whatDescriptor = sft.getDescriptor("What")
        getCQIndexType(whatDescriptor) mustEqual CQIndexType.UNIQUE

        val whenDescriptor = sft.getDescriptor("When")
        getCQIndexType(whenDescriptor) mustEqual CQIndexType.NAVIGABLE

        val whereDescriptor = sft.getDescriptor("Where")
        getCQIndexType(whereDescriptor) mustEqual CQIndexType.NONE

        val whyDescriptor = sft.getDescriptor("Why")
        getCQIndexType(whyDescriptor) mustEqual CQIndexType.HASH
      }

      "via setCQIndexType" >> {
        val originalWhoDescriptor = sft.getDescriptor("Who")
        getCQIndexType(originalWhoDescriptor) mustEqual CQIndexType.DEFAULT

        setCQIndexType(originalWhoDescriptor, CQIndexType.HASH)
        getCQIndexType(originalWhoDescriptor) mustEqual CQIndexType.HASH
      }
    }

  }

}
