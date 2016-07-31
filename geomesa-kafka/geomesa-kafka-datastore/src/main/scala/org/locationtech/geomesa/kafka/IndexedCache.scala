package org.locationtech.geomesa.kafka

import java.util
import java.util.Map.Entry
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.collect.{Maps, Queues}
import com.googlecode.cqengine.attribute.SimpleAttribute
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence
import com.googlecode.cqengine.query.QueryFactory
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Abstracts over a cache, an IndexedCollection, and a spatial index.
  */
class IndexedCache2 extends java.util.AbstractMap[String, FeatureHolder] {

  private val logger = LoggerFactory.getLogger(classOf[IndexedCache2])
  private val mutationQueue = Queues.newLinkedBlockingQueue[Mutation]()

  private val removalListener =  new RemovalListener[String, FeatureHolder] {
    override def onRemoval(notification: RemovalNotification[String, FeatureHolder]): Unit = {
      println(s"Cache triggered removal of ${notification.getKey}")
      mutationQueue.put(Expire(Maps.immutableEntry(notification.getKey, notification.getValue)))
    }
  }
  val cache: Cache[String, FeatureHolder] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(30L, TimeUnit.SECONDS)
      .removalListener(removalListener)
      .build()

  private val setView = cache.asMap().entrySet()
  private val ID_ATTR =
    new SimpleAttribute[Entry[String, FeatureHolder], String]() {
      override def getValue(fh: Entry[String, FeatureHolder], queryOptions: QueryOptions): String = fh.getValue.sf.getID
    }
  private val persistence = OnHeapPersistence.onPrimaryKey(ID_ATTR)
  val index: IndexedCollection[Entry[String, FeatureHolder]] =
    new ConcurrentIndexedCollection[Entry[String, FeatureHolder]](persistence)
  index.addIndex(HashIndex.onAttribute(ID_ATTR))

  private trait Mutation {
    def apply(): Unit
  }
  private case class AddFeature(key: String, fh: FeatureHolder) extends Mutation {
    def apply() = {
      index.add(Maps.immutableEntry(key, fh))
      cache.put(key, fh)
    }
  }
  private case class RemoveFeature(e: Entry[String, FeatureHolder]) extends Mutation {
    def apply() = {
      index.remove(e)
      cache.invalidate(e.getKey)
    }
  }
  private case class Expire(e: Entry[String, FeatureHolder]) extends Mutation {
    def apply() = {
      index.remove(e)
    }
  }

  private val mutationES = Executors.newSingleThreadExecutor()
  private val mutator = new Runnable {
    override def run(): Unit = {
      try {
        while(true) {
          val mutation = mutationQueue.poll()
          if(mutation != null) {
            println(s"Applying $mutation")
            mutation.apply()
          }
        }
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
        case t: Throwable =>
          t.printStackTrace()
      }
    }
  }
  mutationES.submit(mutator)

  override def entrySet(): util.Set[Entry[String, FeatureHolder]] = setView

  override def put(key: String, value: FeatureHolder): FeatureHolder = {
    mutationQueue.put(AddFeature(key, value))
    value
  }

  override def get(key: java.lang.Object): FeatureHolder = {
    val q = QueryFactory.equal(ID_ATTR, key.toString)
    Try(index.retrieve(q).uniqueResult()).map(_.getValue).getOrElse(null)
  }
}
