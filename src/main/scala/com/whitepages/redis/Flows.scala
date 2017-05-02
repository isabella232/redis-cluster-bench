package com.whitepages.redis

import java.util
import java.util.UUID

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.codahale.metrics.Meter
import com.whitepages.tokenbucket.BucketBuilder
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

trait SimpleRedisClient[K,V] {
  def get(key: K): Future[Option[V]]
  def set(key: K, value: V): Future[Boolean]
  def mget(keys: Seq[K]): Future[Seq[Option[V]]]
  def mset(pairs: Map[K, V]): Future[Boolean]

  def getFlow(concurrency: Int): Flow[K, Option[V], NotUsed] =
    Flow[K].mapAsyncUnordered(concurrency)(entry => {
      get(entry)
    })
  def setFlow(concurrency: Int): Flow[(K, V), Boolean, NotUsed] =
    Flow[(K, V)].mapAsyncUnordered(concurrency)(entry => {
      set(entry._1, entry._2)
    })
  def mgetFlow(concurrency: Int): Flow[Seq[K], Seq[Option[V]], NotUsed] =
    Flow[Seq[K]].mapAsyncUnordered(concurrency)(entry => {
      mget(entry)
    })
  def msetFlow(concurrency: Int): Flow[Map[K,V], Boolean, NotUsed] =
    Flow[Map[K,V]].mapAsyncUnordered(concurrency)(entry => {
      mset(entry)
    })
}

case class RedisScalaClusterClient(connectStr: String) extends SimpleRedisClient[String, String] {

  val initialHosts = connectStr.split(""",\s*""").map(_.split(":")).map(node => new HostAndPort(node(0), node(1).toInt)).toSet
  val clusterClient = new JedisCluster(initialHosts.asJava)

  override def get(key: String): Future[Option[String]] =
    Future{ Option(clusterClient.get(key)) }

  override def set(key: String, value: String): Future[Boolean] =
    Future{ Option(clusterClient.set(key, value)).getOrElse("") == "OK" }

  override def mget(keys: Seq[String]): Future[Seq[Option[String]]] =
    Future { clusterClient.mget(keys:_*).asScala.map(Option(_)) }

  override def mset(pairs: Map[String, String]): Future[Boolean] = {
    val keyValues = pairs.toList.flatMap { case (k, v) => List(k, v) }
    Future {
      clusterClient.mset(keyValues:_*) == "OK"
    }
  }
}

object Flows {

  case class RedisEntry(key: String, value: Array[Byte])
  private val keyFormat = "key:%012d"
  def randomValue(payloadSize: Int) = {
    val bytes = new Array[Byte](payloadSize)
    Random.nextBytes(bytes)
    bytes
  }

  def sequentialKeys(count: Int): Iterator[String] =
    Iterator.range(0, count).map(i => keyFormat.format(i))
  def randomKeys(count:Int, keyspaceSize: Int): Iterator[String] =
    Iterator.range(0, count).map(_ => keyFormat.format(Random.nextInt(keyspaceSize)))
  def randomKeys(count:Int): Iterator[String] =
    Iterator.range(0, count).map(_ => UUID.randomUUID().toString)

  def randomValues(payloadSize: Int): Iterator[Array[Byte]] = Iterator.continually(randomValue(payloadSize))

  def recordAndPrintSet(meter: Meter, printEvery: FiniteDuration): Sink[Boolean, Future[Done]] = {
    var lastPrint = System.nanoTime()
    Sink.foreach[Boolean]( success => {
      if (!success) throw new RuntimeException("Failed to set")

      meter.mark()
      if ((System.nanoTime() - lastPrint).nanos > printEvery) {
        lastPrint = System.nanoTime()
        println(s"Count: ${meter.getCount}, Rate: ${meter.getOneMinuteRate}")
      }
    })
  }
  def recordAndPrintGet(hits: Meter, misses: Meter, printEvery: FiniteDuration): Sink[Option[String], Future[Done]] = {
    var lastPrint = System.nanoTime()
    Sink.foreach[Option[String]]( result => {
      if (result.isEmpty) misses.mark() else hits.mark()
      if ((System.nanoTime() - lastPrint).nanos > printEvery) {
        lastPrint = System.nanoTime()
        println(s"Hits: ${hits.getCount}, Misses: ${misses.getCount}, Rate: ${hits.getOneMinuteRate + misses.getOneMinuteRate}")
      }
    })
  }
  def recordAndPrintMGet(hits: Meter, misses: Meter, printEvery: FiniteDuration): Sink[Seq[Option[String]], Future[Done]] = {
    var lastPrint = System.nanoTime()
    Sink.foreach[Seq[Option[String]]]( result => {
      val found = result.count(_.nonEmpty)
      val missed = result.size - found
      if (found > 0) hits.mark(found)
      if (missed > 0) misses.mark(missed)
      if ((System.nanoTime() - lastPrint).nanos > printEvery) {
        lastPrint = System.nanoTime()
        println(s"Hits: ${hits.getCount}, Misses: ${misses.getCount}, Rate: ${hits.getOneMinuteRate + misses.getOneMinuteRate}")
      }
    })
  }

  def rateLimit[T](rate: Int)(implicit ec: ExecutionContext): Flow[T, T, NotUsed] = {
    val rateLimiter =
      if (rate > 0)
        Some(BucketBuilder.strict((1.0/rate).seconds, 1))
      else
        None
    Flow[T].mapAsync(1)( x => {
      rateLimiter.map( limiter => {
        limiter.tokenF(1).map(_ => x)
      }).getOrElse(Future.successful(x))
    })
  }
}
