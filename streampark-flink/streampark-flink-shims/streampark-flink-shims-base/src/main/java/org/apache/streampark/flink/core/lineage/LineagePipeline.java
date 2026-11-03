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

package org.apache.streampark.flink.core.lineage;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * One sink dataset and exactly the input datasets reachable to it through the compiled plan (not
 * to any sibling sink in the same job).
 *
 * <p>A {@code STATEMENT SET} with N independent {@code INSERT} statements compiles into N disjoint
 * connected components in the plan graph. Flattening all of them into one shared input set and one
 * shared output set would produce a full N&times;M cross product of edges when Gravitino stores the
 * resulting OpenLineage events, most of which would be false — table A's data may never actually
 * reach table B's sink. Keeping one {@link LineagePipeline} per sink, each with only the inputs its
 * own subgraph reaches, avoids that. A job with a single {@code INSERT} degenerates to one pipeline
 * with N inputs, same as before.
 *
 * <p>Must remain {@link Serializable} for the same reason as {@link LineageDataset}.
 */
public final class LineagePipeline implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LineageDataset output;
    private final Set<LineageDataset> inputs;

    public LineagePipeline(LineageDataset output, Set<LineageDataset> inputs) {
        this.output = output;
        this.inputs = Collections.unmodifiableSet(new LinkedHashSet<>(inputs));
    }

    public LineageDataset output() {
        return output;
    }

    public Set<LineageDataset> inputs() {
        return inputs;
    }
}
