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
import org.apache.streampark.connector.doris.conf.DorisConfig;
import org.apache.streampark.flink.connector.doris.bean.DorisSinkBufferEntry;
import org.apache.streampark.flink.connector.doris.bean.DorisSinkRowDataWithMeta;
import org.apache.streampark.flink.core.scala.StreamingContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/** DorisSinkFunction */
public class DorisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(DorisSinkFunction.class);
  private final DorisSinkWriter dorisSinkWriter;
  private final DorisConfig dorisConfig;
  // state only works with `EXACTLY_ONCE`
  private transient ListState<Map<String, DorisSinkBufferEntry>> checkpointState;
  private transient Counter totalInvokeRowsTime;
  private transient Counter totalInvokeRows;
  private static final String COUNTER_INVOKE_ROWS_COST_TIME = "totalInvokeRowsTimeNs";
  private static final String COUNTER_INVOKE_ROWS = "totalInvokeRows";

  public DorisSinkFunction(StreamingContext context) {
    Properties properties = context.parameter().getProperties();
    this.dorisConfig = new DorisConfig(properties);
    this.dorisSinkWriter = new DorisSinkWriter(dorisConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    dorisSinkWriter.setRuntimeContext(getRuntimeContext());
    totalInvokeRows = getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS);
    totalInvokeRowsTime =
        getRuntimeContext().getMetricGroup().counter(COUNTER_INVOKE_ROWS_COST_TIME);
    dorisSinkWriter.startScheduler();
    dorisSinkWriter.startAsyncFlushing();
  }

  @Override
  public void invoke(T value, SinkFunction.Context context) throws Exception {
    long start = System.nanoTime();
    if (value instanceof DorisSinkRowDataWithMeta) {
      DorisSinkRowDataWithMeta data = (DorisSinkRowDataWithMeta) value;
      if (StringUtils.isEmpty(data.getDatabase())
          || StringUtils.isEmpty(data.getTable())
          || null == data.getDataRows()) {
        LOGGER.warn(
            String.format(
                " row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                data.getDatabase(), data.getTable(), Arrays.toString(data.getDataRows())));
        return;
      }
      dorisSinkWriter.writeRecords(data.getDatabase(), data.getTable(), data.getDataRows());
    } else {
      if (StringUtils.isEmpty(dorisConfig.database()) || StringUtils.isEmpty(dorisConfig.table())) {
        throw new RuntimeException(
            " database|table  is empt ,please check your config or create DorisSinkRowDataWithMeta instance");
      }
      dorisSinkWriter.writeRecords(dorisConfig.database(), dorisConfig.table(), (String) value);
    }
    // raw data sink
    totalInvokeRows.inc(1);
    totalInvokeRowsTime.inc(System.nanoTime() - start);
  }

  @Override
  public void close() throws Exception {
    super.close();
    dorisSinkWriter.close();
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (Semantic.EXACTLY_ONCE.equals(Semantic.of(dorisConfig.semantic()))) {
      // save state
      checkpointState.add(dorisSinkWriter.getBufferedBatchMap());
      flushPreviousState();
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    if (Semantic.EXACTLY_ONCE.equals(Semantic.of(dorisConfig.semantic()))) {
      ListStateDescriptor<Map<String, DorisSinkBufferEntry>> descriptor =
          new ListStateDescriptor<>(
              "buffered-rows",
              TypeInformation.of(new TypeHint<Map<String, DorisSinkBufferEntry>>() {}));
      checkpointState = context.getOperatorStateStore().getListState(descriptor);
    }
  }

  private void flushPreviousState() throws Exception {
    // flush the batch saved at the previous checkpoint
    for (Map<String, DorisSinkBufferEntry> state : checkpointState.get()) {
      dorisSinkWriter.setBufferedBatchMap(state);
      dorisSinkWriter.flush(null, true);
    }
    checkpointState.clear();
  }
}
