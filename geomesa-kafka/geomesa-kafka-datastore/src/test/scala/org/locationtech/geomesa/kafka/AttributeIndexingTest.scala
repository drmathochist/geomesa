/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import com.google.common.base.Ticker
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.{Query, QueryFactory => CQF}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.query
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language._
import scala.util.Random

class AttributeIndexingTest {
  implicit def sfToCreate(feature: SimpleFeature): CreateOrUpdate = CreateOrUpdate(Instant.now, feature)

  //
  implicit val ff = CommonFactoryFinder.getFilterFactory2

  val spec = "Who:String:index=full,What:Integer,When:Date,*Where:Point:srid=4326,Why:String"
  val MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val seconds_per_year = 365L * 24L * 60L * 60L
  val string = "foo"

  def randDate = MIN_DATE.plusSeconds(scala.math.round(scala.util.Random.nextFloat * seconds_per_year)).toDate

  val sft = SimpleFeatureTypes.createType("test", spec)
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
    builder.set("When", randDate)
    builder.set("Where", getPoint)
    if (Random.nextBoolean()) {
      builder.set("Why", string)
    }
    builder.buildFeature(i.toString)
  }

  def mean(values: Seq[Long]): Double = {
    values.sum.toDouble / values.length.toDouble
  }

  def sd(values: Seq[Long]): Double = {
    val mn = mean(values)
    math.sqrt(
      values.map(x => math.pow(x.toDouble - mn, 2.0)).sum /
        (values.length - 1).toDouble)
  }

  def fd(value: Double): String = {
    "%.1f".format(value)
  }

  def runQueries[T](n: Int, genIter: T => Long, filters: Seq[T]) = {
    println(Seq(
      "c_max",
      "c_min",
      "t_max",
      "t_mean",
      "t_sd",
      "t_min",
      "filter"
    ).mkString("\t"))
    for (f <- filters) {
      val timeRes = (1 to n).map(i => time(genIter(f)))
      val counts = timeRes.map(_._1)
      val times = timeRes.map(_._2)
      println(Seq(
        counts.max,
        counts.min,
        times.max,
        fd(mean(times)),
        fd(sd(times)),
        times.min,
        f.toString
      ).mkString("\t"))
    }
  }

  def time[A](a: => A) = {
    val now = System.currentTimeMillis()
    val result = a
    //println("%f seconds".format( (System.nanoTime - now) / 1000000000.0 ))

    (result, System.currentTimeMillis() - now)
  }

  def timeUnit[Unit](a: => Unit) = {
    val now = System.currentTimeMillis()
    a
    System.currentTimeMillis() - now
  }

  val ab = ECQL.toFilter("Who IN('Addams', 'Bierce')")
  val cd = ECQL.toFilter("Who IN('Clemens', 'Damon')")
  val ab_cd = ff.and(ab, cd)

  val w14 = ECQL.toFilter("What = 1 OR What = 2 OR What = 3 or What = 4")

  val where = ECQL.toFilter("BBOX(Where, 0, 0, 180, 90)")
  val where2 = ECQL.toFilter("BBOX(Where, -180, -90, 0, 0)")

  val posIDL = ECQL.toFilter("BBOX(Where, 170, 0, 180, 10)")
  val negIDL = ECQL.toFilter("BBOX(Where, -180, 0, -170, 10)")
  val idl = ff.or(posIDL, negIDL)

  val abIDL = ff.and(idl, ab)

  val posAB = ff.and(posIDL, ab)
  val negAB = ff.and(negIDL, ab)
  val abIDL2 = ff.or(posAB, negAB)

  val bbox2 = ff.or(where, where2)

  val justified = ECQL.toFilter("Why is not null")

  val justifiedAB = ff.and(ff.and(ab, w14), justified)
  val justifiedCD = ff.and(ff.and(cd, w14), justified)

  val just = ff.or(justifiedAB, justifiedCD)

  val justBBOX = ff.and(just, where)
  val justBBOX2 = ff.and(just, where2)

  val justOR = ff.or(justBBOX, justBBOX2)

  val niceAnd = ff.and(justOR, just)

  val niceAnd2 = ff.and(bbox2, just)

  val geoJustAB = ff.and(justifiedAB, bbox2)
  val geoJustCD = ff.and(justifiedCD, bbox2)
  val badOr = ff.or(geoJustAB, geoJustCD)

  val filters = Seq(ab, cd, w14, where, justified, justifiedAB, justifiedCD, just, justBBOX, justBBOX2, bbox2)
  //val ab_w14 = ff.and(ab, w14)
  //val filters = Seq(ab, cd, w14, ab_w14)

  // Easier filters
  val geoCD = ff.and(cd, bbox2)
  val geoAB = ff.and(ab, bbox2)

  val abcd = ff.or(ab, cd)

  val abcdWhere = ff.and(abcd, where)
  val abcdWhere2 = ff.and(abcd, where2)

  val badOr3 = ff.or(geoCD, geoAB)
  val niceAnd3 = ff.and(abcd, bbox2)
  val jor = ff.or(abcdWhere, abcdWhere2)

  // One geom
  val whereAB = ff.and(where, ab)
  val whereCD = ff.and(where, cd)

  val bad1 = ff.or(whereAB, whereCD)
  val nice1 = ff.and(where, abcd)

  val nFeats = 100000
  val feats = (0 until nFeats).map(buildFeature)
  val featsUpdate = (0 until nFeats).map(buildFeature)


  // load different LiveFeatureCache implementations
  implicit val ticker = Ticker.systemTicker()
  val lfc = new LiveFeatureCacheGuava(sft, None)
  //val h2  = new LiveFeatureCacheH2(sft)
  val cq  = new LiveFeatureCacheCQEngine(sft, None)

  val sfv = new SimplifyingFilterVisitor

  def benchmark(f: Filter) {
    println("Running f")
    val (regularCount, t1) = time(lfc.getReaderForFilter(f).getIterator.size)

    println("\n\nRunning filter in CNF")
    val cnf = rewriteFilterInCNF(f)
    val (cnfCount, tcnf) = time(lfc.getReaderForFilter(cnf.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)

    println("\n\nRunning filter in DNF")
    val dnf = rewriteFilterInDNF(f)
    val (dnfCount, tdnf) = time(lfc.getReaderForFilter(dnf.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)

    println("\n\nRunning filter in 'unoptimized'")
    //val (simpleCount, t2) = time(lfc.getReaderForFilter(f.accept(sfv, null).asInstanceOf[Filter]).getIterator.size)
    val (unoptimizedCount, t3) = time(lfc.unoptimized(f).getIterator.size)

    println(s"\nFilter: $f")
    if (regularCount == cnfCount && regularCount == dnfCount && regularCount == unoptimizedCount) {
      if (t3 < t1) {
        println("'Unoptimized' was quicker than regular")
      }
      if (tcnf < t1) {
        println("'CNF' was quicker than regular")
      }
      if (tdnf < t1) {
        println("'DNF' was quicker than regular")
      }
      println(s"All filters returned $regularCount")

    } else {
      println(s"MISMATCHED Counts: Regular: $regularCount CNF: $cnfCount DNF: $dnfCount  Unoptimized: $unoptimizedCount")
    }
    println(s"Timings: regular: $t1 CNF: $tcnf DNF: $tdnf unoptimized: $t3\n")
  }

  def toC(f: Filter) = {
    f match {
      case or: Or => or.getChildren.toIndexedSeq
      case _ => Seq(f)
    }
  }

  // f is bigger than g
  def contains(f: Filter, g: Filter): Boolean = {
    val fc = toC(f)
    val gc = toC(g)
    fc.contains(gc)
  }


  def printBoolean(f: Filter): String = {
    var int = 0
    val map = new mutable.HashMap[Int, String]()
    val abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray

    def printBooleanInternal(f: Filter): String = {
      f match {
        case a: And => "(" + a.getChildren.map(printBooleanInternal).mkString("+") + ")"
        case o: Or => "(" + o.getChildren.map(printBooleanInternal).mkString("*") + ")"
        case _ =>
          println(s"F: ${f.hashCode()} + $map")
          map.get(f.hashCode) match {
            case Some(v) => v
            case None => {
              val ret = abc(int).toString
              println(s"map.put(${f.hashCode()}, $ret)")
              map.put(f.hashCode(), ret)
              int += 1
              ret
            }
          }
      }
    }
    printBooleanInternal(f)
  }

  def countPopulate(count: Int, time: Long): String = {
    "%d in %d ms (%.1f /ms)".format(count, time, count.toDouble / time)
  }

  def benchmarkLFC() = {
    val lfc_pop = timeUnit(feats.foreach {
      lfc.createOrUpdateFeature(_)
    })
    println("lfc populate: "+countPopulate(feats.size, lfc_pop))

    //runQueries[Filter](11, f => lfc.getReaderForFilter(f).getIterator.size, filters)

    val lfc_repop = timeUnit(featsUpdate.foreach {
      lfc.createOrUpdateFeature(_)
    })
    println("lfc repopulate: "+countPopulate(featsUpdate.size, lfc_repop))

    runQueries[Filter](11, f => lfc.getReaderForFilter(f).getIterator.size, filters)
  }

  /*
  def benchmarkH2() = {
    val h2_pop = timeUnit(for (sf <- feats) {
      h2.createOrUpdateFeature(sf)
    })
    println("h2 populate time (ms) = " + h2_pop)

    // run queries
    runQueries[Filter](11, f => h2.getFeatures(f).features.size, filters)

    //update some of the features
    val h2_repop = timeUnit(for (sf <- featsUpdate) {
      h2.createOrUpdateFeature(sf)
    })
    println("h2 repopulate time (ms) = " + h2_repop)

    // run queries again
    runQueries[Filter](11, f => h2.getFeatures(f).features.size, filters)
  }
  */

  object CQData {

    //val ID = cq.ID
    val WHO_ATTR = cq.attrs.lookup[String]("Who")
    val WHAT_ATTR = cq.attrs.lookup[Integer]("What")
    val WHERE_ATTR = cq.attrs.lookup[Geometry]("Where")

    val ab = CQF.or(CQF.equal(WHO_ATTR, "Addams"), CQF.equal(WHO_ATTR, "Bierce"))
    val cd = CQF.or(CQF.equal(WHO_ATTR, "Clemens"), CQF.equal(WHO_ATTR, "Damon"))
    //val ab_cd = and(ab, cd)
    val w14 = CQF.or(
      CQF.equal[SimpleFeature, Integer](WHAT_ATTR, 1),
      CQF.equal[SimpleFeature, Integer](WHAT_ATTR, 2),
      CQF.equal[SimpleFeature, Integer](WHAT_ATTR, 3),
      CQF.equal[SimpleFeature, Integer](WHAT_ATTR, 4))
    val ab_w14 = CQF.and(ab, w14)


    // Geo - CQEngine mojo
    val qo = new QueryOptions

    import com.googlecode.cqengine.persistence.support._

    val obset = ObjectSet.fromCollection(feats)

    val bboxGeom = WKTUtils.read("POLYGON((0 0, 0 90, 180 90, 180 0, 0 0))")

    val geoIndex = new GeoIndex(WHERE_ATTR)
    geoIndex.addAll(obset, qo)

    val intersectsQuery = new query.Intersects(WHERE_ATTR, bboxGeom)

    val results = geoIndex.retrieve(intersectsQuery, qo)

    val queries = Seq(ab, cd, w14, ab_w14, intersectsQuery)
  }

  def benchmarkCQ() = {
    val cq_pop = timeUnit({
      for (sf <- feats) cq.createOrUpdateFeature(sf)
    })
    println(s"cq populate: ${feats.size} in $cq_pop ms (${feats.size.toDouble / cq_pop}/ms)")
    //println(s"cache size: ${cqholder.cqcache.size}")

    runQueries[Filter](11, f => cq.getReaderForFilter(f).getIterator.size, filters)

    val cq_repop = timeUnit({
      for (sf <- featsUpdate) cq.createOrUpdateFeature(sf)
    })
    println(s"cq repopulate = ${featsUpdate.size} in $cq_repop ms (${featsUpdate.size.toDouble / cq_repop}/ms)")

    runQueries[Filter](11, f => cq.getReaderForFilter(f).getIterator.size, filters)
  }
}

class GraphVizFilterVisitor extends FilterVisitor {
  override def visit(filter: And, extraData: scala.Any): AnyRef = {
    val count = extraDataToCount(extraData)

    printOperand(filter, "AND", count)
    //filter.getChildren.foreach(_.accept(this, count + 1))
    linkChildren(filter, count)
  }

  override def visit(filter: Or, extraData: scala.Any): AnyRef = {
    val count = extraDataToCount(extraData)

    printOperand(filter, "OR", count)
    //filter.getChildren.foreach(_.accept(this, count + 1))
    linkChildren(filter, count)
  }

  def linkChildren(binary: BinaryLogicOperator, count: Int) = {
    binary.getChildren.foreach { c =>
      val childRand = math.abs(scala.util.Random.nextInt())
      println(s"node_${nodeName(binary, count)} -> node_${nodeName(c, childRand)}")
      c.accept(this, childRand)
    }
    null
  }

  def printOperand(filter: Filter, op: String, count: Int) = {
    println(s"""node_${nodeName(filter, count)} [ label="$op" shape="rectangle"]""")

  }

  override def visit(filter: Not, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(metBy: MetBy, extraData: scala.Any): AnyRef = printRectangle(metBy, extraData)
  override def visit(meets: Meets, extraData: scala.Any): AnyRef = printRectangle(meets, extraData)
  override def visit(ends: Ends, extraData: scala.Any): AnyRef = printRectangle(ends, extraData)
  override def visit(endedBy: EndedBy, extraData: scala.Any): AnyRef = printRectangle(endedBy, extraData)
  override def visit(begunBy: BegunBy, extraData: scala.Any): AnyRef = printRectangle(begunBy, extraData)
  override def visit(begins: Begins, extraData: scala.Any): AnyRef = printRectangle(begins, extraData)
  override def visit(anyInteracts: AnyInteracts, extraData: scala.Any): AnyRef = printRectangle(anyInteracts, extraData)

  override def visit(contains: TOverlaps, extraData: scala.Any): AnyRef = printRectangle(contains, extraData)
  override def visit(equals: TEquals, extraData: scala.Any): AnyRef = printRectangle(equals, extraData)
  override def visit(contains: TContains, extraData: scala.Any): AnyRef = printRectangle(contains, extraData)
  override def visit(overlappedBy: OverlappedBy, extraData: scala.Any): AnyRef = printRectangle(overlappedBy, extraData)
  override def visit(filter: Touches, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: Overlaps, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(during: During, extraData: scala.Any): AnyRef = printRectangle(during, extraData)
  override def visit(before: Before, extraData: scala.Any): AnyRef = printRectangle(before, extraData)
  override def visit(after: After, extraData: scala.Any): AnyRef = printRectangle(after, extraData)
  override def visit(filter: Id, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: Equals, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: IncludeFilter, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: DWithin, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: Within, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsLessThanOrEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsLessThan, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsGreaterThanOrEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsGreaterThan, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsNotEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)
  override def visit(filter: PropertyIsBetween, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: ExcludeFilter, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsLike, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsNull, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsNil, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: BBOX, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Beyond, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Contains, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Crosses, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Disjoint, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visitNullFilter(extraData: scala.Any): AnyRef = ??? // printRectangle(filter, extraData)

  override def visit(filter: Intersects, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  def printRectangle(filter: Filter, extraData: scala.Any) = {
    val count = extraDataToCount(extraData)
    println(s"""node_${nodeName(filter, count)} [ label="${filter.toString}" shape="rectangle"]""")
    null
  }

  def extraDataToCount(extraData: scala.Any): Int = {
    extraData match {
      case i: Int => i
      case _ => 0
    }
  }

  def nodeName(filter: Filter, count: Int): String = {
    s"${count}_${filter.hashCode().toLong.toHexString}"
  }
}
