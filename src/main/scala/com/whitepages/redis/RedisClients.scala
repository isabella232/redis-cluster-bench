package com.whitepages.redis

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.lambdaworks.redis.{ReadFrom, RedisURI}
import org.redisson.Redisson
import org.redisson.api.RBucket
import org.redisson.config.{Config, ReadMode}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Future

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

  def shutdown: Unit
}

case class JedisClusterClient(connectStr: String) extends SimpleRedisClient[String, String] {
  import redis.clients.jedis.{HostAndPort, JedisCluster}

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

  def shutdown: Unit = clusterClient.close()
}

case class LettuceClusterClient(connectStr: String) extends SimpleRedisClient[String, String] {
  import com.lambdaworks.redis.RedisURI
  import com.lambdaworks.redis.cluster.ClusterTopologyRefreshOptions.RefreshTrigger
  import com.lambdaworks.redis.cluster.{ClusterClientOptions, ClusterTopologyRefreshOptions, RedisClusterClient}


  val initialHosts = connectStr.split(""",\s*""").map(_.split(":")).map(node => RedisURI.create(node(0), node(1).toInt)).toSet
  val clusterClient = RedisClusterClient.create(initialHosts.asJava)
  val topologyRefreshOptions =
    ClusterTopologyRefreshOptions
      .builder()
      .enableAdaptiveRefreshTrigger(RefreshTrigger.MOVED_REDIRECT, RefreshTrigger.PERSISTENT_RECONNECTS)
      .adaptiveRefreshTriggersTimeout(30, TimeUnit.SECONDS)
      .build()
  
  clusterClient.setOptions(
    ClusterClientOptions.builder()
      .topologyRefreshOptions(topologyRefreshOptions)
      //.pingBeforeActivateConnection(true) // prevents shutdown?
      .requestQueueSize(10000)  // per server connection
      .build()
  )
  val connection = clusterClient.connect()
  connection.setReadFrom(ReadFrom.NEAREST)
  val syncApi = connection.sync()
  syncApi.ping()

  override def get(key: String): Future[Option[String]] =
    Future { Option(syncApi.get(key)) }

  override def set(key: String, value: String): Future[Boolean] =
    Future { Option(syncApi.set(key, value)).getOrElse("") == "OK" }

  override def mget(keys: Seq[String]): Future[Seq[Option[String]]] =
    Future { syncApi.mget(keys:_*).asScala.map(Option(_)) }

  override def mset(pairs: Map[String, String]): Future[Boolean] =
    Future { syncApi.mset(pairs.asJava) == "OK" }

  override def shutdown: Unit = {
    syncApi.close()
    clusterClient.shutdown()
  }
}

case class RedissonClusterClient(connectStr: String) extends SimpleRedisClient[String, String] {
  val initialHosts = connectStr.split(""",\s*""")
  val config = new Config().setCodec(new org.redisson.client.codec.StringCodec())
  config.useClusterServers()
    .setReadMode(ReadMode.MASTER_SLAVE)
    .addNodeAddress(initialHosts:_*)
  val client = Redisson.create(config)


  override def get(key: String): Future[Option[String]] =
    Future { Option(client.getBucket[String](key).get()) }

  override def set(key: String, value: String): Future[Boolean] =
    Future { client.getBucket[String](key).set(value); true }

  override def mget(keys: Seq[String]): Future[Seq[Option[String]]] = Future {
    val batch = client.createBatch()
    keys.foreach(batch.getBucket[String](_).getAsync)
    val result =
      batch
      .execute()
    result
      .asScala
      .map(x => Option(x).map(_.asInstanceOf[String]))
      
  }

  override def mset(pairs: Map[String, String]): Future[Boolean] = Future {
    val batch = client.createBatch()
    pairs.toList.map{ case (k, v) => batch.getBucket[String](k).setAsync(v) }
    batch.execute
    true
  }

  override def shutdown: Unit = client.shutdown()
}
