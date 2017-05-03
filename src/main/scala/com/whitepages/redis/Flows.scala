package com.whitepages.redis

import java.util
import java.util.UUID

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.codahale.metrics.Meter
import com.whitepages.tokenbucket.BucketBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global


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
    Flow[T].mapAsyncUnordered(1)( x => {
      rateLimiter.map( limiter => {
        limiter.tokenF(1).map(_ => x)
      }).getOrElse(Future.successful(x))
    })
  }
}
