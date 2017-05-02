package com.whitepages.redis

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Source}
import com.codahale.metrics.Meter

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Benchmark extends App {

  case class CLIConfig(
                        connectString: String = "",
                        clientType: String = "cluster",
                        clients: Int = 1,
                        putSize: Int = 2,
                        requests: Int = 1,
                        keyspaceLen: Int = 10000,
                        mode: String = "fill",
                        keySource: String = "sequential",
                        bulk: Int = 0,
                        rateLimit: Int = 0
                      )

  val cliParser = new scopt.OptionParser[CLIConfig]("redis-benchmark") {
    help("help") text ("print this usage text")
    opt[String]('h', "host") required() action { (x, c) => {
      c.copy(connectString = x)
    }} text ("Redis node")
    opt[Int]('c', "clients") optional() action { (x, c) => {
      c.copy(clients = x)
    }} text ("Number of concurrent requests to allow")
    opt[String]('t', "type") optional() action { (x, c) => {
      c.copy(clientType = x)
    }} text ("Redis instance type")
    opt[String]("keysource") required() action { (x, c) => {
      c.copy(keySource = x)
    }} text ("One of: sequential, random-bounded, uuid, file")
    opt[Int]('r', "keyspacelen") optional() action { (x, c) => {
      c.copy(keyspaceLen = x)
    }} text ("Use random IDs from [0-keyspacelen], only used if fillmode is 'random'")
    opt[Int]('n', "requests") required() action { (x, c) => {
      c.copy(requests = x)
    }} text ("Number of entities to add")
    opt[Int]('b', "bulk") optional() action { (x, c) => {
      c.copy(bulk = x)
    }} text ("Bulk mode, use mset or mget instead of set or get")
    opt[Int]("rate") optional() action { (x, c) => {
      c.copy(rateLimit = x)
    }} text ("Limit operations to this rate per second")
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

      val keySource: () => Iterator[String] = () => config.keySource.toLowerCase match {
        case "sequential" =>
          Flows.sequentialKeys(config.requests)
        case "random-bounded" =>
          Flows.randomKeys(config.requests, config.keyspaceLen)
        case "uuid" =>
          Flows.randomKeys(config.requests)
        case "file" =>
          scala.io.Source.fromFile("input.txt").getLines()  // TODO
        case _ =>
          throw new RuntimeException("Unrecognized fill mode: " + config.keySource)
      }
      try {
        val client: SimpleRedisClient[String,String] = config.clientType match {
          case "jediscluster" => RedisScalaClusterClient(config.connectString)
        }
        config.mode match {

          case "fill" => {
            val rate = new Meter()
            val data: () => Iterator[(String,String)] = () => config.keySource.toLowerCase match {
              case "file" =>
                keySource().map(line => {
                  val fields = line.split("\t")
                  (fields(0), fields(1))
                })
              case _ =>
                keySource().zip(Flows.randomValues(config.putSize).map(new String(_, "UTF-8")))
            }

            val source =
              Source.fromIterator(data)
                .buffer(config.clients, OverflowStrategy.backpressure)
            val job =
              if (config.bulk <= 0)
                source
                  .via(client.setFlow(config.clients))
                  .via(Flows.rateLimit[Boolean](config.rateLimit))
                  .toMat(Flows.recordAndPrintSet(rate, 5.second))(Keep.right)
              else
                source
                  .grouped(config.bulk)
                  .map(_.toMap)
                  .via(client.msetFlow(config.clients))
                  .via(Flows.rateLimit[Boolean](config.rateLimit))
                  .toMat(Flows.recordAndPrintSet(rate, 5.second))(Keep.right)

            val startTime = System.nanoTime()
            Await.result(job.run, Duration.Inf)
            val duration = (System.nanoTime() - startTime).nanos

            println(s"Count: ${rate.getCount}")
            if (duration.toSeconds > 0) println(s"Rate: ${rate.getCount / duration.toSeconds}")
            println(s"Done in ${duration.toUnit(TimeUnit.SECONDS)} seconds")
          }
          case "get" => {
            val hits = new Meter()
            val misses = new Meter()

            val source = Source.fromIterator(keySource)
              .buffer(config.clients, OverflowStrategy.backpressure)
            
            val job =
              if (config.bulk <= 0)
                source
                  .via(client.getFlow(config.clients))
                  .via(Flows.rateLimit[Option[String]](config.rateLimit))
                  .toMat(Flows.recordAndPrintGet(hits, misses, 5.second))(Keep.right)
              else
                source
                  .grouped(config.bulk)
                  .via(client.mgetFlow(config.clients))
                  .via(Flows.rateLimit[Seq[Option[String]]](config.rateLimit))
                  .toMat(Flows.recordAndPrintMGet(hits, misses, 5.second))(Keep.right)

            val startTime = System.nanoTime()
            Await.result(job.run, Duration.Inf)
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
