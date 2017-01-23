package com.whitepages.redis

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Source
import com.codahale.metrics.Meter
import com.whitepages.redis.Flows.RedisEntry
import redis.{RedisClientPool, RedisCluster, RedisServer}

import scala.concurrent.Await
import scala.concurrent.duration._

object Benchmark extends App {

  case class CLIConfig(
                        node: String = "",
                        port: Int = 6379,
                        cluster: Boolean = false,
                        clients: Int = 1,
                        putSize: Int = 2,
                        requests: Int = 1,
                        keyspaceLen: Int = 10000,
                        mode: String = "fill",
                        keySource: String = "sequential"
                      )

  val cliParser = new scopt.OptionParser[CLIConfig]("redis-benchmark") {
    help("help") text ("print this usage text")
    opt[String]('h', "host") required() action { (x, c) => {
      c.copy(node = x)
    }} text ("Redis node")
    opt[Int]("port") optional() action { (x, c) => {
      c.copy(port = x)
    }} text ("Redis port")
    opt[Int]('c', "clients") optional() action { (x, c) => {
      c.copy(clients = x)
    }} text ("Number of concurrent requests to allow")
    opt[Unit]("cluster") optional() action { (x, c) => {
      c.copy(cluster = true)
    }} text ("Cluster mode")
    opt[String]("keysource") required() action { (x, c) => {
      c.copy(keySource = x)
    }} text ("One of: sequential, random-bounded, uuid, file")
    opt[Int]('r', "keyspacelen") optional() action { (x, c) => {
      c.copy(keyspaceLen = x)
    }} text ("Use random IDs from [0-keyspacelen], only used if fillmode is 'random'")
    opt[Int]('n', "requests") required() action { (x, c) => {
      c.copy(requests = x)
    }} text ("Number of entities to add")
    cmd("fill") action { (_, c) =>
      c.copy(mode = "fill") } text("Put random data into the cluster") children(
      opt[Int]('d', "size") optional() action { (x, c) => {
        c.copy(putSize = x)
      }} text ("Data size in bytes")
    )
    cmd("get") action { (_, c) =>
      c.copy(mode = "get") } text("Retrieval performance testing") children(

    )
  }

  cliParser.parse(args, CLIConfig()).fold({
    // argument error, the parser should have already informed the user
  })({
    config => {
      implicit val system = ActorSystem("redis-benchmark")
      implicit val materializer = ActorMaterializer()

      val servers = config.node.split(""",\s*""").map(node => RedisServer(node, config.port))
      val keySource: () => Iterator[String] = () => config.keySource.toLowerCase match {
        case "sequential" =>
          Flows.sequentialKeys(config.requests)
        case "random-bounded" =>
          Flows.randomKeys(config.requests, config.keyspaceLen)
        case "uuid" =>
          Flows.randomKeys(config.requests)
        case "file" =>
          throw new RuntimeException("TODO")
        case _ =>
          throw new RuntimeException("Unrecognized fill mode: " + config.keySource)
      }
      try {
        config.mode match {

          case "fill" => {
            val rate = new Meter()
            val data: () => Iterator[RedisEntry] = () => config.keySource.toLowerCase match {
              case "file" =>
                throw new RuntimeException("TODO")
              case _ =>
                keySource().zip(Flows.randomValues(config.putSize)).map { case (k, v) => RedisEntry(k, v) }
            }

            val job =
              Source.fromIterator(data)
                .buffer(config.clients, OverflowStrategy.backpressure)
                .via(
                  if (config.cluster)
                    Flows.sendToRedis(RedisCluster(servers), config.clients)
                  else
                    Flows.sendToRedis(RedisClientPool(servers), config.clients)
                )

            val startTime = System.nanoTime()
            Await.result(job.runWith(Flows.recordAndPrint(rate, 5.second)), Duration.Inf)
            val duration = (System.nanoTime() - startTime).nanos

            println(s"Count: ${rate.getCount}")
            if (duration.toSeconds > 0) println(s"Rate: ${rate.getCount / duration.toSeconds}")
            println(s"Done in ${duration.toUnit(TimeUnit.SECONDS)} seconds")
          }
          case "get" => {
            val hits = new Meter()
            val misses = new Meter()

            val job =
              Source.fromIterator(keySource)
                .buffer(config.clients, OverflowStrategy.backpressure)
                .via(
                  if (config.cluster)
                    Flows.getFromRedis(RedisCluster(servers), config.clients)
                  else
                    Flows.getFromRedis(RedisClientPool(servers), config.clients)
                )

            val startTime = System.nanoTime()
            Await.result(job.runWith(Flows.recordAndPrint(hits, misses, 5.second)), Duration.Inf)
            val duration = (System.nanoTime() - startTime).nanos

            println(s"Hits: ${hits.getCount}")
            println(s"Misses: ${misses.getCount}")
            if (duration.toSeconds > 0) println(s"Rate: ${(hits.getCount + misses.getCount) / duration.toSeconds}")
            println(s"Done in ${duration.toUnit(TimeUnit.SECONDS)} seconds")

          }
        }
      }
      finally {
        system.terminate()
      }
    }
  })
}
