/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.flink.kubernetes

import org.apache.streampark.common.util.{Logger, Utils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.kubernetes.model._

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

import java.util.Objects
import java.util.concurrent.TimeUnit

/** Tracking info cache pool on flink kubernetes mode. */
class FlinkK8sWatchController(conf: JobStatusWatcherConfig = JobStatusWatcherConfig.defaultConf) extends Logger with AutoCloseable {

  // cache for tracking identifiers
  lazy val trackIds: TrackIdCache = TrackIdCache.build()

  lazy val canceling: TrackIdCache = TrackIdCache.build()

  // cache for flink Job-manager rest url
  lazy val endpoints: EndpointCache = EndpointCache.build()

  // cache for tracking flink job status
  lazy val jobStatuses: JobStatusCache = JobStatusCache.build(conf.jobStatusCacheTimeOutSec)

  // cache for tracking kubernetes events with Deployment kind
  lazy val k8sDeploymentEvents: K8sDeploymentEventCache =
    K8sDeploymentEventCache.build()

  // cache for last each flink cluster metrics (such as a session cluster or a application cluster)
  lazy val flinkMetrics: MetricCache = MetricCache.build()

  override def close(): Unit = {
    jobStatuses.cleanUp()
    k8sDeploymentEvents.cleanUp()
    trackIds.cleanUp()
  }

  /** collect all tracking identifiers */
  def getAllWatchingIds(): Set[TrackId] = trackIds.getAll()

  /** determines whether the specified TrackId is in the trace */
  def isInWatching(trackId: TrackId): Boolean = {
    if (!trackId.isLegal) false;
    else {
      trackIds.get(trackId) != null
    }
  }

  def unWatching(trackId: TrackId): Unit = {
    if (trackId.isLegal) {
      trackIds.invalidate(trackId)
      canceling.invalidate(trackId)
      jobStatuses.invalidate(trackId)
      flinkMetrics.invalidate(ClusterKey.of(trackId))
    }
  }

  /** collect all legal tracking ids, and covert to ClusterKey */
  private[kubernetes] def getActiveWatchingIds(): Set[TrackId] =
    getAllWatchingIds().filter(_.isActive)

  /** collect the aggregation of flink metrics that in tracking */
  def collectAccGroupMetric(groupId: String): FlinkMetricCV = {
    // get cluster metrics that in tracking
    val empty = FlinkMetricCV.empty(groupId)
    getActiveWatchingIds().filter(_.groupId == groupId) match {
      case k if k.isEmpty => empty
      case k =>
        flinkMetrics.getAll(
          for (elem <- k if elem.groupId == groupId)
            yield ClusterKey.of(elem)) match {
          case m if m.isEmpty => empty
          case m => m.values.fold(empty)((x, y) => x + y)
        }
    }
  }

  /** get flink job-manager rest url from cache which will auto refresh when it it empty. */
  def getClusterRestUrl(clusterKey: ClusterKey): Option[String] = {
    Option(endpoints.get(clusterKey))
      .filter(_.nonEmpty)
      .orElse(refreshClusterRestUrl(clusterKey))
  }

  /** refresh flink job-manager rest url from remote flink cluster, and cache it. */
  def refreshClusterRestUrl(clusterKey: ClusterKey): Option[String] = {
    val restUrl = KubernetesRetriever.retrieveFlinkRestUrl(clusterKey)
    if (restUrl.nonEmpty) {
      endpoints.put(clusterKey, restUrl.get)
    }
    restUrl
  }

}

/**
 * cache ======
 */
case class CacheKey(key: java.lang.Long) extends Serializable {
  override def hashCode(): Int = Utils.hashCode(key)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: CacheKey => Objects.equals(key, that.key)
      case _ => false
    }
  }
}

class TrackIdCache {

  private[this] lazy val cache: Cache[CacheKey, TrackId] =
    Caffeine.newBuilder.build()

  def update(k: TrackId): Unit = {
    val key = CacheKey(k.appId)
    cache.invalidate(key)
    cache.put(key, k)
  }

  def set(k: TrackId): Unit = cache.put(CacheKey(k.appId), k)

  def invalidate(k: TrackId): Unit = cache.invalidate(CacheKey(k.appId))

  def get(k: TrackId): TrackId = cache.getIfPresent(CacheKey(k.appId))

  def has(k: TrackId): Boolean = get(k) != null

  def getAll(): Set[TrackId] = cache.asMap().values().toSet

  def cleanUp(): Unit = cache.cleanUp()

}

object TrackIdCache {
  def build(): TrackIdCache = {
    new TrackIdCache()
  }
}

class JobStatusCache(timeout: Int) {

  private[this] lazy val cache: Cache[CacheKey, JobStatusCV] =
    Caffeine.newBuilder.expireAfterWrite(timeout, TimeUnit.SECONDS).build()

  def putAll(kvs: Map[TrackId, JobStatusCV]): Unit =
    cache.putAll(kvs.map(t => (CacheKey(t._1.appId), t._2)))

  def put(k: TrackId, v: JobStatusCV): Unit = cache.put(CacheKey(k.appId), v)

  def asMap(): Map[CacheKey, JobStatusCV] = cache.asMap().toMap

  def getAsMap(trackIds: Set[TrackId]): Map[CacheKey, JobStatusCV] =
    cache.getAllPresent(trackIds.map(t => t.appId)).toMap

  def get(k: TrackId): JobStatusCV = {
    cache.getIfPresent(CacheKey(k.appId))
  }

  def invalidate(k: TrackId): Unit = cache.invalidate(CacheKey(k.appId))

  def cleanUp(): Unit = cache.cleanUp()

}

object JobStatusCache {

  def build(timeout: Int): JobStatusCache = new JobStatusCache(timeout)

}

class EndpointCache {

  private[this] lazy val cache: Cache[ClusterKey, String] =
    Caffeine.newBuilder().expireAfterWrite(24, TimeUnit.HOURS).build()

  def invalidate(k: ClusterKey): Unit = cache.invalidate(k)

  def put(k: ClusterKey, v: String): Unit = cache.put(k, v)

  def get(key: ClusterKey): String = cache.getIfPresent(key)

}

object EndpointCache {

  def build(): EndpointCache = new EndpointCache()

}

class K8sDeploymentEventCache {
  def put(k: K8sEventKey, v: K8sDeploymentEventCV): Unit = cache.put(k, v)

  def get(k: K8sEventKey): K8sDeploymentEventCV = cache.getIfPresent(k)

  def asMap(): Map[K8sEventKey, K8sDeploymentEventCV] = cache.asMap().toMap

  def cleanUp(): Unit = cache.cleanUp()

  val cache: Cache[K8sEventKey, K8sDeploymentEventCV] =
    Caffeine.newBuilder.build()

}

object K8sDeploymentEventCache {
  def build(): K8sDeploymentEventCache = new K8sDeploymentEventCache()

}

class MetricCache {

  private[this] lazy val cache: Cache[ClusterKey, FlinkMetricCV] =
    Caffeine.newBuilder().build()

  def put(k: ClusterKey, v: FlinkMetricCV): Unit = cache.put(k, v)

  def asMap(): Map[ClusterKey, FlinkMetricCV] = cache.asMap().toMap

  def getAll(k: Set[ClusterKey]): Map[ClusterKey, FlinkMetricCV] =
    cache.getAllPresent(k).toMap

  def get(key: ClusterKey): FlinkMetricCV = cache.getIfPresent(key)

  def invalidate(key: ClusterKey): Unit = cache.invalidate(key)

}

object MetricCache {

  def build(): MetricCache = new MetricCache()

}
