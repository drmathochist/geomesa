/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.language._
import scala.util.Random

object SampleFeatures {
  val spec = List(
    "Who:String:index=full",
    "What:Integer",
    "WhatLong:Long",
    "WhatFloat:Float",
    "WhatDouble:Double",
    "When:Date",
    "*Where:Point:srid=4326",
    "Why:String"
  ).mkString(",")
  val sft = SimpleFeatureTypes.createType("test", spec)

  val specIndexes = List(
    "Who:String:cq_index=default",
    "What:Integer:cq_index=navigable",
    "When:Date:cq_index=navigable",
    "*Where:Point:srid=4326",
    "Why:String"  // Why can have nulls
  ).mkString(",")
  val sftWithIndexes = SimpleFeatureTypes.createType("test2", specIndexes)

  val cq = SFTAttributes(sft)
  val ff = CommonFactoryFinder.getFilterFactory2

  val MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val seconds_per_year = 365L * 24L * 60L * 60L
  val string = "foo"

  def randDate = MIN_DATE.plusSeconds(scala.math.round(scala.util.Random.nextFloat * seconds_per_year)).toDate

  val builder = new SimpleFeatureBuilder(sft)

  val names = Array("Addams", "Bierce", "Clemens", "Damon", "Evan", "Fred", "Goliath", "Harry")

  def getName: String = names(Random.nextInt(names.length))

  def getPoint: Point = {
    val minx = -180
    val miny = -90
    val dx = 360
    val dy = 180

    val x = minx + Random.nextDouble * dx
    val y = miny + Random.nextDouble * dy
    WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  }

  def buildFeature(i: Int): SimpleFeature = {
    builder.set("Who", getName)
    builder.set("What", Random.nextInt(10))
    builder.set("WhatLong", Random.nextInt(10).toLong)
    builder.set("WhatFloat", Random.nextFloat * 10.0F)
    builder.set("WhatDouble", Random.nextDouble() * 10.0)
    builder.set("When", randDate)
    builder.set("Where", getPoint)
    if (Random.nextBoolean()) {
      builder.set("Why", string)
    }
    builder.buildFeature(i.toString)
  }
}
