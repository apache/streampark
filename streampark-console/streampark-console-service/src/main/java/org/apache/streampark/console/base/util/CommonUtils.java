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

package org.apache.streampark.console.base.util;

import org.apache.streampark.common.util.AssertUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cglib.beans.BeanMap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public final class CommonUtils implements Serializable {

  private CommonUtils() {}

  private static final long serialVersionUID = 1L;

  private static final String OS = System.getProperty("os.name").toLowerCase();

  /**
   * is empty
   *
   * @param objs handle obj
   * @return Boolean
   * @see <b>Returns true if the object is Null, returns true if the size of the collection is 0,
   *     and returns true if the iterator has no next</b>
   * @since 1.0
   */
  public static Boolean isEmpty(Object... objs) {

    if (objs == null) {
      return Boolean.TRUE;
    }

    if (objs.length == 0) {
      return Boolean.TRUE;
    }

    for (Object obj : objs) {
      if (obj == null) {
        return true;
      }

      // char sequence
      if ((obj instanceof CharSequence) && "".equals(obj.toString().trim())) {
        return true;
      }
      // collection
      if (obj instanceof Collection) {
        if (((Collection<?>) obj).isEmpty()) {
          return true;
        }
      }
      // map
      if (obj instanceof Map) {
        if (((Map<?, ?>) obj).isEmpty()) {
          return true;
        }
      }

      if (obj instanceof Iterable) {
        if (((Iterable<?>) obj).iterator() == null || !((Iterable<?>) obj).iterator().hasNext()) {
          return true;
        }
      }

      // iterator
      if (obj instanceof Iterator) {
        if (!((Iterator<?>) obj).hasNext()) {
          return true;
        }
      }

      // file
      if (obj instanceof File) {
        if (!((File) obj).exists()) {
          return true;
        }
      }

      if ((obj instanceof Object[]) && ((Object[]) obj).length == 0) {
        return true;
      }
    }

    return false;
  }

  /**
   * non-empty
   *
   * @param obj handle obj
   * @return Boolean
   * @since 1.0
   */
  public static Boolean notEmpty(Object... obj) {
    return !isEmpty(obj);
  }

  public static Long toLong(Object val, Long defVal) {
    if (isEmpty(val)) {
      return defVal;
    }
    try {
      return Long.parseLong(val.toString());
    } catch (NumberFormatException e) {
      return defVal;
    }
  }

  public static Long toLong(Object val) {
    return toLong(val, null);
  }

  public static Integer toInt(Object val, Integer defVal) {
    if (isEmpty(val)) {
      return defVal;
    }
    try {
      return Integer.parseInt(val.toString());
    } catch (NumberFormatException e) {
      return defVal;
    }
  }

  public static float toFloat(Object val, float defVal) {
    if (isEmpty(val)) {
      return defVal;
    }
    try {
      return Float.parseFloat(val.toString());
    } catch (NumberFormatException e) {
      return defVal;
    }
  }

  public static Boolean toBoolean(String text, Boolean defVal) {
    if (isEmpty(text)) {
      return false;
    }
    try {
      return Boolean.parseBoolean(text);
    } catch (NumberFormatException e) {
      return defVal;
    }
  }

  public static Boolean toBoolean(String text) {
    return toBoolean(text, false);
  }

  public static Integer toInt(Object val) {
    return toInt(val, null);
  }

  public static Float toFloat(Object val) {
    return toFloat(val, 0f);
  }

  public static List arrayToList(Object source) {
    return Arrays.asList(ObjectUtils.toObjectArray(source));
  }

  public static boolean contains(Iterator iterator, Object element) {
    if (iterator != null) {
      while (iterator.hasNext()) {
        Object candidate = iterator.next();
        if (ObjectUtils.safeEquals(candidate, element)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check whether the given Enumeration contains the given element.
   *
   * @param enumeration the Enumeration to check
   * @param element the element to look for
   * @return <code>true</code> if found, <code>false</code> else
   */
  public static boolean contains(Enumeration enumeration, Object element) {
    if (enumeration != null) {
      while (enumeration.hasMoreElements()) {
        Object candidate = enumeration.nextElement();
        if (ObjectUtils.safeEquals(candidate, element)) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean deleteFile(File dir) {
    if (!dir.exists()) {
      return false;
    }
    if (dir.isDirectory()) {
      for (File file : dir.listFiles()) {
        if (!deleteFile(file)) {
          return false;
        }
      }
    }
    return dir.delete();
  }

  /**
   * Check whether the given Collection contains the given element instance.
   *
   * <p>Enforces the given instance to be present, rather than returning <code>true</code> for an
   * equal element as well.
   *
   * @param collection the Collection to check
   * @param element the element to look for
   * @return <code>true</code> if found, <code>false</code> else
   */
  public static boolean containsInstance(Collection collection, Object element) {
    if (collection != null) {
      for (Object candidate : collection) {
        if (candidate == element) {
          return true;
        }
      }
    }
    return false;
  }

  public static <A, E extends A> A[] toArray(Enumeration<E> enumeration, A[] array) {
    ArrayList<A> elements = new ArrayList<A>();
    while (enumeration.hasMoreElements()) {
      elements.add(enumeration.nextElement());
    }
    return elements.toArray(array);
  }

  /**
   * Adapt an enumeration to an iterator.
   *
   * @param enumeration the enumeration
   * @return the iterator
   */
  public static <E> Iterator<E> toIterator(Enumeration<E> enumeration) {
    @SuppressWarnings("hiding")
    class EnumerationIterator<E> implements Iterator<E> {
      private final Enumeration<E> enumeration;

      public EnumerationIterator(Enumeration<E> enumeration) {
        this.enumeration = enumeration;
      }

      @Override
      public boolean hasNext() {
        return this.enumeration.hasMoreElements();
      }

      @Override
      public E next() {
        return this.enumeration.nextElement();
      }

      @Override
      public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported");
      }
    }

    return new EnumerationIterator<E>(enumeration);
  }

  public static String getOsName() {
    return OS;
  }

  public static boolean isLinux() {
    return OS.indexOf("linux") >= 0;
  }

  public static boolean isMacOS() {
    return OS.indexOf("mac") >= 0 && OS.indexOf("os") > 0 && OS.indexOf("x") < 0;
  }

  public static boolean isMacOSX() {
    return OS.indexOf("mac") >= 0 && OS.indexOf("os") > 0 && OS.indexOf("x") > 0;
  }

  public static boolean isWindows() {
    return OS.indexOf("windows") >= 0;
  }

  public static boolean isOS2() {
    return OS.indexOf("os/2") >= 0;
  }

  public static boolean isSolaris() {
    return OS.indexOf("solaris") >= 0;
  }

  public static boolean isSunOS() {
    return OS.indexOf("sunos") >= 0;
  }

  public static boolean isMPEiX() {
    return OS.indexOf("mpe/ix") >= 0;
  }

  public static boolean isHPUX() {
    return OS.indexOf("hp-ux") >= 0;
  }

  public static boolean isAix() {
    return OS.indexOf("aix") >= 0;
  }

  public static boolean isOS390() {
    return OS.indexOf("os/390") >= 0;
  }

  public static boolean isFreeBSD() {
    return OS.indexOf("freebsd") >= 0;
  }

  public static boolean isIrix() {
    return OS.indexOf("irix") >= 0;
  }

  public static boolean isDigitalUnix() {
    return OS.indexOf("digital") >= 0 && OS.indexOf("unix") > 0;
  }

  public static boolean isNetWare() {
    return OS.indexOf("netware") >= 0;
  }

  public static boolean isOSF1() {
    return OS.indexOf("osf1") >= 0;
  }

  public static boolean isOpenVMS() {
    return OS.indexOf("openvms") >= 0;
  }

  public static boolean isUnix() {
    boolean isUnix = isLinux();
    if (!isUnix) {
      isUnix = isMacOS();
    }
    if (!isUnix) {
      isUnix = isMacOSX();
    }
    if (!isUnix) {
      isUnix = isLinux();
    }
    if (!isUnix) {
      isUnix = isDigitalUnix();
    }
    if (!isUnix) {
      isUnix = isAix();
    }
    if (!isUnix) {
      isUnix = isFreeBSD();
    }
    if (!isUnix) {
      isUnix = isHPUX();
    }
    if (!isUnix) {
      isUnix = isIrix();
    }
    if (!isUnix) {
      isUnix = isMPEiX();
    }
    if (!isUnix) {
      isUnix = isNetWare();
    }
    if (!isUnix) {
      isUnix = isOpenVMS();
    }
    if (!isUnix) {
      isUnix = isOS2();
    }
    if (!isUnix) {
      isUnix = isOS390();
    }
    if (!isUnix) {
      isUnix = isOSF1();
    }
    if (!isUnix) {
      isUnix = isSunOS();
    }
    if (!isUnix) {
      isUnix = isSolaris();
    }
    return isUnix;
  }

  /** linux kernel platform 1 window: 2 other platforms 0 */
  public static int getPlatform() {
    int platform = 0;
    if (CommonUtils.isUnix()) {
      platform = 1;
    }
    if (CommonUtils.isWindows()) {
      platform = 2;
    }
    return platform;
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValue(Map<K, V> map) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, Comparator.comparing(Map.Entry::getValue));
    Map<K, V> result = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  public static <T> T[] arrayRemoveElements(T[] array, T... elem) {
    AssertUtils.notNull(array);
    List<T> arrayList = new ArrayList<>(0);
    Collections.addAll(arrayList, array);
    if (isEmpty(elem)) {
      return array;
    }
    for (T el : elem) {
      arrayList.remove(el);
    }
    return Arrays.copyOf(arrayList.toArray(array), arrayList.size());
  }

  public static <T> T[] arrayRemoveIndex(T[] array, int... index) {
    AssertUtils.notNull(array);
    for (int j : index) {
      if (j < 0 || j > array.length - 1) {
        throw new IndexOutOfBoundsException("index error.@" + j);
      }
    }
    List<T> arrayList = new ArrayList<>(0);
    Collections.addAll(arrayList, array);
    int i = 0;
    for (int j : index) {
      arrayList.remove(j - i);
      ++i;
    }
    return Arrays.copyOf(arrayList.toArray(array), arrayList.size());
  }

  public static <T> T[] arrayInsertIndex(T[] array, int index, T t) {
    AssertUtils.notNull(array);
    List<T> arrayList = new ArrayList<T>(array.length + 1);
    if (index == 0) {
      arrayList.add(t);
      Collections.addAll(arrayList, array);

    } else {
      T[] before = Arrays.copyOfRange(array, 0, index);
      T[] after = Arrays.copyOfRange(array, index, array.length);
      Collections.addAll(arrayList, before);
      arrayList.add(t);
      Collections.addAll(arrayList, after);
    }
    return arrayList.toArray(array);
  }

  public static String uuid() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  /**
   * generate specific length uuid
   *
   * @param len len
   * @return uuid
   */
  public static String uuid(int len) {
    StringBuffer sb = new StringBuffer();
    while (sb.length() < len) {
      sb.append(uuid());
    }
    return sb.substring(0, len);
  }

  public static Double fixedNum(Number number) {
    return fixedNum(number, 2);
  }

  public static Double fixedNum(Number number, int offset) {
    if (number.doubleValue() == 0.00) {
      return 0D;
    }
    String prefix = "";
    while (offset > 0) {
      prefix += "0";
      offset -= 1;
    }

    java.text.DecimalFormat df = new java.text.DecimalFormat("#." + prefix);
    try {
      return Double.parseDouble(df.format(number));
    } catch (NumberFormatException e) {
      log.error(e.getMessage(), e);
      return 0.0;
    }
  }

  public static String toPercent(Number number) {
    return toPercent(number, 0);
  }

  public static String toPercent(Number number, int offset) {
    offset += 2;
    Double num = fixedNum(number, offset);
    return (num * 100) + "%";
  }

  /**
   * convert bean to map
   *
   * @param bean bean
   * @return map
   */
  public static <T> Map<String, Object> beanToMap(T bean) {
    Map<String, Object> map = new HashMap<>();
    if (bean != null) {
      BeanMap beanMap = BeanMap.create(bean);
      for (Object key : beanMap.keySet()) {
        map.put(String.valueOf(key), beanMap.get(key));
      }
    }
    return map;
  }

  /**
   * convert map to bean
   *
   * @param map map
   * @param bean bean class
   * @return bean
   */
  public static <T> T mapToBean(Map<String, Object> map, T bean) {
    BeanMap beanMap = BeanMap.create(bean);
    beanMap.putAll(map);
    return bean;
  }

  /**
   * convert List<T> to List<Map<String, Object>>
   *
   * @param objList
   * @return
   * @throws IOException
   */
  public static <T> List<Map<String, Object>> objectsToMaps(List<T> objList) {
    List<Map<String, Object>> list = new ArrayList<>();
    if (objList != null && !objList.isEmpty()) {
      Map<String, Object> map = null;
      T bean = null;
      for (T t : objList) {
        bean = t;
        map = beanToMap(bean);
        list.add(map);
      }
    }
    return list;
  }

  /**
   * convert List<Map<String,Object>> to List<T>
   *
   * @param maps maps
   * @param clazz element class
   * @return
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static <T> List<T> mapsToObjects(List<Map<String, Object>> maps, Class<T> clazz)
      throws InstantiationException, IllegalAccessException {
    List<T> list = new ArrayList<>();
    if (maps != null && !maps.isEmpty()) {
      Map<String, Object> map;
      T bean;
      for (Map<String, Object> stringObjectMap : maps) {
        map = stringObjectMap;
        bean = clazz.newInstance();
        mapToBean(map, bean);
        list.add(bean);
      }
    }
    return list;
  }

  public static Integer getPid() {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    String name = runtime.getName();
    try {
      return Integer.parseInt(name.substring(0, name.indexOf('@')));
    } catch (Exception e) {
      return -1;
    }
  }

  public static boolean isLegalUrl(String url) {
    try {
      new URI(url);
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }
}
