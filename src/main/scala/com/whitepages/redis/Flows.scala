package com.whitepages.redis

import java.util.UUID

import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.codahale.metrics.Meter
import redis.{RedisClientPool, RedisCluster}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object Flows {

  case class RedisEntry(key: String, value: Array[Byte])
  private val keyFormat = "%012d"
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

  def sendToRedis(redis: RedisClientPool, concurrency: Int): Flow[RedisEntry, Boolean, NotUsed] =
    Flow[RedisEntry].mapAsyncUnordered(concurrency)(entry => {
      redis.set(entry.key, entry.value)
    })
  def sendToRedis(redis: RedisCluster, concurrency: Int): Flow[RedisEntry, Boolean, NotUsed] =
    Flow[RedisEntry].mapAsyncUnordered(concurrency)(entry => {
      redis.set(entry.key, entry.value)
    })

  def getFromRedis(redis: RedisClientPool, concurrency: Int): Flow[String, Option[ByteString], NotUsed] = {
    Flow[String].mapAsyncUnordered(concurrency)( key => {
      redis.get(key)
    })
  }
  def getFromRedis(redis: RedisCluster, concurrency: Int): Flow[String, Option[ByteString], NotUsed] = {
    Flow[String].mapAsyncUnordered(concurrency)( key => {
      redis.get(key)
    })
  }

  def recordAndPrint(meter: Meter, printEvery: FiniteDuration): Sink[Boolean, Future[Done]] = {
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
  def recordAndPrint(hits: Meter, misses: Meter, printEvery: FiniteDuration): Sink[Option[ByteString], Future[Done]] = {
    var lastPrint = System.nanoTime()
    Sink.foreach[Option[ByteString]]( result => {
      if (result.isEmpty) misses.mark() else hits.mark()
      if ((System.nanoTime() - lastPrint).nanos > printEvery) {
        lastPrint = System.nanoTime()
        println(s"Hits: ${hits.getCount}, Misses: ${misses.getCount}, Rate: ${hits.getOneMinuteRate + misses.getOneMinuteRate}")
      }
    })
  }


}
