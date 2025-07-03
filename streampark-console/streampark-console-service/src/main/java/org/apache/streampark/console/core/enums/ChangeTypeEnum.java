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

package org.apache.streampark.console.core.enums;

import lombok.Getter;

import java.util.Arrays;

/** This is an enumeration representing the types of changes that can occur. */
@Getter
public enum ChangeTypeEnum {

    /** Represents no change. */
    NONE(0, "[NONE], nothing to changed"),

    /** Represents a change in dependencies. */
    DEPENDENCY(1, "[DEPENDENCY], Dependency has changed"),

    /** Represents a change in SQL. */
    SQL(2, "[SQL], SQL is changed"),

    /** Represents a change in both dependencies and SQL. */
    ALL(3, "[ALL], Dependency and SQL both changed"),

    /** Represents a change in team resources. */
    TEAM_RESOURCE(4, "[TEAM_RESOURCE], Team resource has changed");

    private final int value;
    private final String description;

    /**
     * Constructor for the enum.
     *
     * @param value The integer value of the enum item.
     * @param description A description of the change type.
     */
    ChangeTypeEnum(int value, String description) {
        this.value = value;
        this.description = description;
    }

    /**
     * Returns the enum item that matches the given integer value.
     *
     * @param value The integer value.
     * @return The matching enum item, or null if no match is found.
     */
    public static ChangeTypeEnum of(Integer value) {
        return Arrays.stream(values())
            .filter(changeTypeEnum -> changeTypeEnum.value == value)
            .findFirst()
            .orElse(null);
    }

    /**
     * Checks if there are any changes.
     *
     * @return True if there are any changes, false otherwise.
     */
    public boolean hasChanged() {
        return !(this == NONE);
    }

    /**
     * Checks if there are any dependency changes.
     *
     * @return True if there are dependency changes, false otherwise.
     */
    public boolean isDependencyChanged() {
        return this == ALL || this == DEPENDENCY || this == TEAM_RESOURCE;
    }

    /**
     * Returns the description of the change type.
     *
     * @return The description of the change type.
     */
    @Override
    public String toString() {
        return description;
    }
}
