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

package org.apache.streampark.flink.connector.doris.internal;

import org.apache.streampark.common.enums.Semantic;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.connector.doris.conf.DorisConfig;
import org.apache.streampark.flink.connector.doris.bean.DorisSinkBufferEntry;
import org.apache.streampark.flink.connector.doris.bean.LoadStatusFailedException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DorisSinkWriter implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(DorisSinkWriter.class);

  private final DorisConfig dorisConfig;
  private final DorisStreamLoader streamLoader;

  private ScheduledExecutorService schedule;
  private ScheduledFuture<?> scheduledFuture;

  private transient Counter totalFlushBytes;
  private transient Counter totalFlushRows;
  private transient Counter totalFlushTime;
  private transient Counter totalFlushTimeWithoutRetries;
  private transient Counter totalFlushSucceededTimes;
  private transient Counter totalFlushFailedTimes;

  private final Map<String, DorisSinkBufferEntry> bufferMap = new ConcurrentHashMap<>();
  private volatile boolean closed = false;
  private volatile boolean flushThreadAlive = false;
  private volatile Throwable flushException;

  final LinkedBlockingDeque<DorisSinkBufferEntry> flushQueue = new LinkedBlockingDeque<>(10);

  private static final String COUNTER_TOTAL_FLUSH_BYTES = "totalFlushBytes";
  private static final String COUNTER_TOTAL_FLUSH_ROWS = "totalFlushRows";
  private static final String COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES =
      "totalFlushTimeNsWithoutRetries";
  private static final String COUNTER_TOTAL_FLUSH_COST_TIME = "totalFlushTimeNs";
  private static final String COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES = "totalFlushSucceededTimes";
  private static final String COUNTER_TOTAL_FLUSH_FAILED_TIMES = "totalFlushFailedTimes";

  private final Semantic semantic;

  public DorisSinkWriter(DorisConfig dorisConfig) {
    this.streamLoader = new DorisStreamLoader(dorisConfig);
    this.dorisConfig = dorisConfig;
    semantic = Semantic.of(dorisConfig.semantic());
  }

  public void setRuntimeContext(RuntimeContext runtimeCtx) {
    totalFlushBytes = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_BYTES);
    totalFlushRows = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_ROWS);
    totalFlushTime = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME);
    totalFlushTimeWithoutRetries =
        runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES);
    totalFlushSucceededTimes =
        runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES);
    totalFlushFailedTimes = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_FAILED_TIMES);
  }

  public void startAsyncFlushing() {
    final Thread flushThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  if (!asyncFlush()) {
                    LOG.info("doris flush thread is about to exit.");
                    flushThreadAlive = false;
                    break;
                  }
                } catch (Exception e) {
                  flushException = e;
                }
              }
            });
    flushThread.setUncaughtExceptionHandler(
        (t, e) -> {
          LOG.error("dorics flush thread uncaught exception occurred:" + e.getMessage(), e);
          flushException = e;
          flushThreadAlive = false;
        });
    flushThread.setName("doris-flush");
    flushThread.setDaemon(true);
    flushThread.start();
    flushThreadAlive = true;
  }

  public void startScheduler() {
    if (semantic.equals(Semantic.EXACTLY_ONCE)) {
      return;
    }
    stopSchedule();
    this.schedule =
        Executors.newScheduledThreadPool(1, ThreadUtils.threadFactory("doris-interval-sink"));
    this.scheduledFuture =
        this.schedule.schedule(
            () -> {
              synchronized (DorisSinkWriter.this) {
                if (!closed) {
                  try {
                    LOG.info("doris interval sinking trigger");
                    if (bufferMap.isEmpty()) {
                      startScheduler();
                    }
                    flush(null, false);
                  } catch (Exception e) {
                    flushException = e;
                  }
                }
              }
            },
            dorisConfig.flushInterval(),
            TimeUnit.MILLISECONDS);
  }

  private void stopSchedule() {
    if (this.scheduledFuture != null) {
      scheduledFuture.cancel(false);
      this.schedule.shutdown();
    }
  }

  public final synchronized void writeRecords(String database, String table, String... records)
      throws IOException {
    checkFlushException();
    try {
      if (records.length == 0) {
        return;
      }
      final String bufferKey = String.format("%s.%s", database, table);
      final DorisSinkBufferEntry bufferEntity =
          bufferMap.computeIfAbsent(
              bufferKey, k -> new DorisSinkBufferEntry(database, table, dorisConfig.labelPrefix()));
      for (String record : records) {
        byte[] bts = record.getBytes(StandardCharsets.UTF_8);
        bufferEntity.addToBuffer(bts);
      }
      if (Semantic.EXACTLY_ONCE.equals(semantic)) {
        return;
      }
      if (bufferEntity.getBatchCount() >= dorisConfig.sinkMaxRow()
          || bufferEntity.getBatchSize() >= dorisConfig.sinkMaxBytes()) {
        LOG.info(
            String.format(
                "doris buffer Sinking triggered: db: [%s] table: [%s] rows[%d] label[%s].",
                database, table, bufferEntity.getBatchCount(), bufferEntity.getLabel()));
        flush(bufferKey, false);
      }
    } catch (Exception e) {
      throw new IOException("Writing records to doris failed.", e);
    }
  }

  public synchronized void flush(String bufferKey, boolean waitUntilDone) throws Exception {
    if (bufferMap.isEmpty()) {
      flushInternal(null, waitUntilDone);
      return;
    }
    if (null == bufferKey) {
      for (String key : bufferMap.keySet()) {
        flushInternal(key, waitUntilDone);
      }
      return;
    }
    flushInternal(bufferKey, waitUntilDone);
  }

  private synchronized void flushInternal(String bufferKey, boolean waitUntilDone)
      throws Exception {
    checkFlushException();
    if (null == bufferKey || bufferMap.isEmpty() || !bufferMap.containsKey(bufferKey)) {
      if (waitUntilDone) {
        waitAsyncFlushingDone();
      }
      return;
    }
    offer(bufferMap.get(bufferKey));
    bufferMap.remove(bufferKey);
    if (waitUntilDone) {
      // wait the last flush
      waitAsyncFlushingDone();
    }
  }

  private void waitAsyncFlushingDone() throws InterruptedException {
    // wait for previous flushings
    offer(new DorisSinkBufferEntry(null, null, null));
    checkFlushException();
  }

  private void offer(DorisSinkBufferEntry bufferEntity) throws InterruptedException {
    if (!flushThreadAlive) {
      throw new RuntimeException(
          "Flush thread already exit or not start ,please exec  startAsyncFlushing() , ignore offer request for label[%s] ");
    }
    if (!flushQueue.offer(bufferEntity, dorisConfig.sinkOfferTimeout(), TimeUnit.MILLISECONDS)) {
      throw new RuntimeException(
          "Timeout while offering data to flushQueue, exceed "
              + dorisConfig.sinkOfferTimeout()
              + " ms, see "
              + dorisConfig.sinkOption().sinkOfferTimeout().key());
    }
  }

  private boolean asyncFlush() throws Exception {
    long timeOut = 3000L;
    final DorisSinkBufferEntry flushData = flushQueue.poll(timeOut, TimeUnit.MILLISECONDS);
    if (flushData == null || flushData.getBatchCount() == 0) {
      return true;
    }
    stopSchedule();
    LOG.info(
        String.format(
            "Async stream load: db[%s] table[%s] rows[%d] bytes[%d] label[%s].",
            flushData.getDatabase(),
            flushData.getTable(),
            flushData.getBatchCount(),
            flushData.getBatchSize(),
            flushData.getLabel()));
    long startWithRetries = System.nanoTime();
    for (int i = 0; i < dorisConfig.sinkMaxRetries(); i++) {
      try {
        long start = System.nanoTime();
        streamLoader.doStreamLoad(flushData);
        LOG.info(String.format("Async stream load finished: label[%s].", flushData.getLabel()));
        if (null != totalFlushBytes) {
          totalFlushBytes.inc(flushData.getBatchSize());
          totalFlushRows.inc(flushData.getBatchCount());
          totalFlushTime.inc(System.nanoTime() - startWithRetries);
          totalFlushTimeWithoutRetries.inc(System.nanoTime() - start);
          totalFlushSucceededTimes.inc();
        }
        startScheduler();
        break;
      } catch (Exception e) {
        if (totalFlushFailedTimes != null) {
          totalFlushFailedTimes.inc();
        }
        LOG.warn("Failed to flush batch data to doris, retry times = {}", i, e);
        if (i >= dorisConfig.sinkMaxRetries()) {
          throw e;
        }
        if (e instanceof LoadStatusFailedException
            && ((LoadStatusFailedException) e).needReCreateLabel()) {
          String oldLabel = flushData.getLabel();
          flushData.reGenerateLabel();
          LOG.warn(
              String.format(
                  "Batch label changed from [%s] to [%s]", oldLabel, flushData.getLabel()));
        }
      }
      try {
        Thread.sleep(1000L * (i + 1));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IOException("Unable to flush, interrupted while doing another attempt", ex);
      }
    }
    return true;
  }

  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      LOG.info("Sink is about to close.");
      flush(null, false);
      this.bufferMap.clear();
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
        schedule.shutdown();
      }
    }
    checkFlushException();
  }

  private void checkFlushException() {
    if (flushException != null) {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      for (StackTraceElement stackTraceElement : stack) {
        LOG.info(
            stackTraceElement.getClassName()
                + "."
                + stackTraceElement.getMethodName()
                + " line:"
                + stackTraceElement.getLineNumber());
      }
      throw new RuntimeException("Writing records to doris failed.", flushException);
    }
  }

  public Map<String, DorisSinkBufferEntry> getBufferedBatchMap() {
    return new HashMap<>(bufferMap);
  }

  public void setBufferedBatchMap(Map<String, DorisSinkBufferEntry> newBufferMap) {
    if (Semantic.EXACTLY_ONCE.equals(semantic)) {
      bufferMap.clear();
      bufferMap.putAll(newBufferMap);
    }
  }
}
