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
import java.util.Objects;

/**
 * OpenLineage dataset identity as sent to Gravitino's {@code POST /api/lineage}.
 *
 * <p>Equality is exact string match on (namespace, name) — that is what Gravitino's {@code
 * lineage_dataset} table dedupes on, so the same physical table must resolve to a byte-identical
 * identity everywhere it is produced or consumed, including by other non-StreamPark emitters
 * writing into the same Gravitino graph.
 *
 * <p>Must remain {@link Serializable}: instances cross Flink-version classloader boundaries via
 * {@code FlinkShimsProxy.getObject}, which round-trips objects through Java serialization.
 */
public final class LineageDataset implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;

    public LineageDataset(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LineageDataset)) {
            return false;
        }
        LineageDataset that = (LineageDataset) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }

    @Override
    public String toString() {
        return namespace + "/" + name;
    }
}
