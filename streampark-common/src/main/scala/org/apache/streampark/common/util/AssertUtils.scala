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

import javax.annotation.Nullable

import java.util
import java.util.{Collection => JavaCollection, Map => JavaMap}

import scala.collection.JavaConversions._

/** @since 2.1.6 */
object AssertUtils {

  /**
   * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if the
   * condition is not met (evaluates to {@code false}).
   *
   * @param condition
   *   The condition to check
   * @throws IllegalArgumentException
   *   Thrown, if the condition is violated.
   */
  def required(condition: Boolean): Unit = {
    if (!condition) {
      throw new IllegalArgumentException
    }
  }

  /**
   * Checks the given boolean condition, and throws an {@code IllegalArgumentException} if the
   * condition is not met (evaluates to {@code false}). The exception will have the given error
   * message.
   *
   * @param condition
   *   The condition to check
   * @param message
   *   The message for the {@code IllegalArgumentException} that is thrown if the check fails.
   * @throws IllegalArgumentException
   *   Thrown, if the condition is violated.
   */
  def required(condition: Boolean, @Nullable message: String): Unit = {
    if (!condition) {
      throw new IllegalArgumentException(message)
    }
  }

  /**
   * Checks the given boolean condition, and throws an {@code IllegalStateException} if the
   * condition is not met (evaluates to {@code false}).
   *
   * @param condition
   *   The condition to check
   * @throws IllegalStateException
   *   Thrown, if the condition is violated.
   */
  def state(condition: Boolean): Unit = {
    if (!condition) {
      throw new IllegalStateException
    }
  }

  /**
   * Checks the given boolean condition, and throws an IllegalStateException if the condition is not
   * met (evaluates to {@code false}). The exception will have the given error message.
   *
   * @param condition
   *   The condition to check
   * @param message
   *   The message for the IllegalStateException that is thrown if the check fails.
   * @throws IllegalStateException
   *   Thrown, if the condition is violated.
   */
  def state(condition: Boolean, @Nullable message: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(message)
    }
  }

  // ------------------------------------------------------------------------
  //  Null checks
  // ------------------------------------------------------------------------
  /** Ensures that the given object reference is not null. Upon violation, a */
  def notNull[T](@Nullable reference: T): T = {
    if (reference == null) {
      throw new NullPointerException
    }
    reference
  }

  /**
   * Ensures that the given object reference is not null. Upon violation, a NullPointerException
   * that is thrown if the check fails.
   *
   * @return
   *   The object reference itself (generically typed).
   * @throws NullPointerException
   *   Thrown, if the passed reference was null.
   */
  def notNull[T](@Nullable reference: T, @Nullable message: String): T = {
    if (reference == null) {
      throw new NullPointerException(message)
    }
    reference
  }

  def isEmpty(reference: AnyRef): Boolean = !isNotEmpty(reference)

  def isNotEmpty(elem: AnyRef): Boolean = {
    elem match {
      case null => false
      case x if x.isInstanceOf[Array[_]] => elem.asInstanceOf[Array[_]].nonEmpty
      case x if x.isInstanceOf[CharSequence] => elem.toString.trim.nonEmpty
      case x if x.isInstanceOf[Traversable[_]] => x.asInstanceOf[Traversable[_]].nonEmpty
      case x if x.isInstanceOf[Iterable[_]] => x.asInstanceOf[Iterable[_]].nonEmpty
      case x if x.isInstanceOf[JavaCollection[_]] => !x.asInstanceOf[JavaCollection[_]].isEmpty
      case x if x.isInstanceOf[JavaMap[_, _]] => !x.asInstanceOf[JavaMap[_, _]].isEmpty
      case _ => true
    }
  }

  /**
   * Assert that an Array|CharSequence|JavaCollection|Map|Iterable... must not be {@code null} and
   * must contain at least one element. <pre class="code">AssertUtils.notEmpty(array, "must be
   * contain elements");</pre>
   *
   * @param reference
   *   the object to check
   * @throws IllegalArgumentException
   *   if the object array is {@code null} or contains no elements
   */
  def notEmpty(reference: AnyRef): Unit = {
    if (isEmpty(reference)) {
      throw new IllegalArgumentException()
    }
  }

  /**
   * Assert that an Array|CharSequence|JavaCollection|Map|Iterable... must not be {@code null} and
   * must contain at least one element. <pre class="code"> AssertUtils.notEmpty(array, "must be
   * contain elements");</pre>
   *
   * @param reference
   *   the object to check
   * @param message
   *   the exception message to use if the assertion fails
   * @throws IllegalArgumentException
   *   if the object array is {@code null} or contains no elements
   */
  def notEmpty(@Nullable reference: AnyRef, message: String): Unit = {
    if (isEmpty(reference)) {
      throw new IllegalArgumentException(message)
    }
  }

  /**
   * Assert that an array contains no {@code null} elements. <p>Note: Does not complain if the array
   * is empty! <pre class="code">AssertUtils.noNullElements(array, "The array must contain non-null
   * elements");</pre>
   *
   * @param array
   *   the array to check
   * @param message
   *   the exception message to use if the assertion fails
   * @throws IllegalArgumentException
   *   if the object array contains a {@code null} element
   */
  def noNullElements(@Nullable array: Array[AnyRef], message: String): Unit = {
    if (array != null) for (element <- array) {
      if (element == null) throw new IllegalArgumentException(message)
    }
  }

  /**
   * Assert that a collection contains no {@code null} elements. <p>Note: Does not complain if the
   * collection is empty! <pre class="code">AssertUtils.noNullElements(collection, "Collection must
   * contain non-null elements");</pre>
   *
   * @param collection
   *   the collection to check
   * @param message
   *   the exception message to use if the assertion fails
   * @throws IllegalArgumentException
   *   if the collection contains a {@code null} element
   */
  def noNullElements(@Nullable collection: util.Collection[_], message: String): Unit = {
    if (collection != null) for (element <- collection) {
      if (element == null) {
        throw new IllegalArgumentException(message)
      }
    }
  }

  /**
   * Assert that the given String is not empty; that is, it must not be {@code null} and not the
   * empty String. <pre class="code">AssertUtils.hasLength(name, "Name must not be empty");</pre>
   *
   * @param text
   *   the String to check
   * @throws IllegalArgumentException
   *   if the text is empty
   * @see
   *   StringUtils#hasLength
   */
  def hasLength(@Nullable text: String): Unit = {
    if (!getHasLength(text)) {
      throw new IllegalArgumentException()
    }
  }

  /**
   * Assert that the given String is not empty; that is, it must not be {@code null} and not the
   * empty String. <pre class="code">AssertUtils.hasLength(name, "Name must not be empty");</pre>
   *
   * @param text
   *   the String to check
   * @param message
   *   the exception message to use if the assertion fails
   * @throws IllegalArgumentException
   *   if the text is empty
   * @see
   *   StringUtils#hasLength
   */
  def hasLength(@Nullable text: String, message: String): Unit = {
    if (!getHasLength(text)) {
      throw new IllegalArgumentException(message)
    }
  }

  /**
   * Assert that the given String contains valid text content; that is, it must not be {@code null}
   * and must contain at least one non-whitespace character. <pre
   * class="code">AssertUtils.hasText(name, "'name' must not be empty");</pre>
   *
   * @param text
   *   the String to check
   * @throws IllegalArgumentException
   *   if the text does not contain valid text content
   * @see
   *   StringUtils#hasText
   */
  def hasText(@Nullable text: String): Unit = {
    if (!getHasText(text)) {
      throw new IllegalArgumentException()
    }
  }

  /**
   * Assert that the given String contains valid text content; that is, it must not be {@code null}
   * and must contain at least one non-whitespace character. <pre
   * class="code">AssertUtils.hasText(name, "'name' must not be empty");</pre>
   *
   * @param text
   *   the String to check
   * @param message
   *   the exception message to use if the assertion fails
   * @throws IllegalArgumentException
   *   if the text does not contain valid text content
   * @see
   *   StringUtils#hasText
   */
  def hasText(@Nullable text: String, message: String): Unit = {
    if (!getHasText(text)) {
      throw new IllegalArgumentException(message)
    }
  }

  private[this] def getHasLength(@Nullable str: String): Boolean =
    str != null && str.nonEmpty

  private[this] def getHasText(@Nullable str: String): Boolean = {
    str != null && str.nonEmpty && containsText(str)
  }

  private[this] def containsText(str: CharSequence): Boolean = {
    val strLen = str.length
    for (i <- 0 until strLen) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return true
      }
    }
    false
  }

}
