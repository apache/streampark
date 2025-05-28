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

package org.apache.streampark.common.util

import org.apache.streampark.common.util.Implicits.{AutoCloseImplicits, StringImplicits}

import org.scalatest.funsuite.AnyFunSuite

class ImplicitsTest extends AnyFunSuite {
  test(
    "AutoCloseImplicits.using should close the resource after execution and handle exceptions") {
    class MockResource extends AutoCloseable {
      var closed = false
      def close(): Unit = closed = true
    }

    def operation(resource: MockResource): Unit = {
      throw new RuntimeException("Simulated exception")
    }

    val mockResource = new MockResource
    assertThrows[RuntimeException] {
      mockResource.using(operation)
    }
    assert(mockResource.closed)
  }

  test("StringImplicits.cast should convert string to the specified type") {
    val byteString = "1"
    val intString = "123"
    val doubleString = "3.14"
    val floatString = "3.14f"
    val booleanString = "true"
    val shortString = "12345"
    val longString = "100000000"

    assert(booleanString.cast[Boolean](classOf[Boolean]))
    assert(byteString.cast[Byte](classOf[Byte]) == 1)
    assert(intString.cast[Int](classOf[Int]) == 123)
    assert(doubleString.cast[Double](classOf[Double]) == 3.14)
    assert(floatString.cast[Float](classOf[Float]) == 3.14f)
    assert(shortString.cast[Short](classOf[Short]) == 12345)
    assert(longString.cast[Long](classOf[Long]) == 100000000)
  }

  test("StringImplicits.cast should throw IllegalArgumentException for unsupported type") {
    val unsupportedString = "test"
    val unsupportedBigdeciaml = "10e5"
    assertThrows[IllegalArgumentException] {
      unsupportedString.cast[Unit](classOf[Unit])
      unsupportedBigdeciaml.cast[BigDecimal](classOf[BigDecimal])
    }
  }
}
