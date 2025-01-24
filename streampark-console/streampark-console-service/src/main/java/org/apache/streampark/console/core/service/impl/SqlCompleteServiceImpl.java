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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.console.core.service.SqlCompleteService;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SqlCompleteServiceImpl implements SqlCompleteService {

  private static final Set<Character> BLACK_SET = Sets.newHashSet(' ', ';');

  private static SqlCompleteFstTree completeFstTree;

  @PostConstruct
  public void initialize() {
    completeFstTree = new SqlCompleteFstTree();
  }

  @Override
  public List<String> getComplete(String sql) {
    if (!sql.isEmpty() && BLACK_SET.contains(sql.charAt(sql.length() - 1))) {
      return new ArrayList<>();
    }
    String[] temp = sql.split("\\s");
    return completeFstTree.getComplicate(temp[temp.length - 1]);
  }

  /** maintain a TreeSet sorting object in the positive order of occurrence frequency */
  private static class WordWithFrequency implements Comparable<WordWithFrequency> {
    String word;
    Integer count;

    public WordWithFrequency(String word, int count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public int compareTo(WordWithFrequency other) {
      int num = this.count - other.count;

      // -1 just for ascending output
      if (num == 0) {
        // This step is very critical. If the same name is not judged and the count is different,
        // then the set collection will default to the same element, and it will be overwritten.
        return this.word.compareTo(other.word) * -1;
      } else {
        return num * -1;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WordWithFrequency that = (WordWithFrequency) o;
      return Objects.equals(word, that.word) && Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
      return Objects.hash(word, count);
    }
  }

  private static class SqlCompleteFstTree {

    // symbol reminder
    private static final String CHARACTER_NOTICE = "()\t<>\t\"\"\t''\t{}";

    // file separator
    private static final String SPLIT_CHAR = "\t";
    private static final TreeNode READ_FROM_HEAD = new TreeNode(' ');
    private static final TreeNode READ_FROM_TAIL = new TreeNode(' ');

    public SqlCompleteFstTree() {
      try {
        Resource resource = new ClassPathResource("sql-rev.dict");
        Scanner scanner = new Scanner(resource.getInputStream());
        StringBuilder stringBuffer = new StringBuilder();
        while (scanner.hasNextLine()) {
          stringBuffer.append(scanner.nextLine()).append(SPLIT_CHAR);
        }
        scanner.close();
        String dict = stringBuffer.toString();
        Arrays.stream(dict.split(SPLIT_CHAR))
            .map(e -> e.trim().toLowerCase())
            .forEach(e -> this.initSearchTree(e, 1));
      } catch (IOException e) {
        log.error("FstTree require reserved word init fail, {}", e.getMessage());
      }

      Arrays.stream(CHARACTER_NOTICE.split(SPLIT_CHAR))
          .map(e -> e.trim().toLowerCase())
          .forEach(e -> this.initSearchTree(e, 1));

      try {
        Resource resource = new ClassPathResource("sql-statistics.dict");
        Scanner scanner = new Scanner(resource.getInputStream());
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          String[] sqlStat = line.split(SPLIT_CHAR);
          this.initSearchTree(sqlStat[0], Integer.parseInt(sqlStat[1].trim()));
        }
        scanner.close();
      } catch (Exception e) {
        log.info("Error while FstTree ini that: {}", e.getMessage());
      }
    }

    /** used to return FST depth */
    private static class Single {
      public Integer loc = 0;
    }

    /**
     * Used to initialize positive and reverse dictionary trees
     *
     * @param word word
     * @param count frequency
     */
    public void initSearchTree(String word, int count) {
      this.buildTree(word, count, READ_FROM_HEAD);
      this.buildTree(new StringBuffer(word).reverse().toString(), count, READ_FROM_TAIL);
    }

    /**
     * @param word single word
     * @param count word frequency
     * @param buildWay root node
     */
    public void buildTree(String word, int count, TreeNode buildWay) {
      int loc = 0;
      Map<Character, TreeNode> nowStep = buildWay.getNext();
      TreeNode preNode = null;
      while (loc < word.length()) {
        Character nowChar = word.charAt(loc);
        if (!nowStep.containsKey(nowChar)) {
          TreeNode temp = new TreeNode(nowChar);
          nowStep.put(nowChar, temp);
        }
        preNode = nowStep.get(nowChar);
        nowStep = nowStep.get(nowChar).getNext();
        loc += 1;
      }
      AssertUtils.notNull(preNode);
      preNode.setStop();
      preNode.setCount(count);
    }

    /**
     * Traverse the FST tree and return potential word nodes
     *
     * @param word associative words
     * @param searchWay current FST tree traversal node
     * @param breakLoc Incoming variables, recording the depth of recursion, used to measure the
     *     effectiveness of the association
     * @return return the last potential node traversed
     */
    private List<TreeNode> getMaybeNodeList(
        String word, TreeNode searchWay, SqlCompleteFstTree.Single breakLoc) {
      int loc = 0;
      Map<Character, TreeNode> nowStep = searchWay.getNext();
      while (loc < word.length()) {
        Character nowChar = word.charAt(loc);
        if (!nowStep.containsKey(nowChar)) {
          // maybe wrong typing
          breakLoc.loc = loc;
          break;
        }
        nowStep = nowStep.get(nowChar).getNext();
        loc += 1;
      }
      breakLoc.loc = loc;
      // At least there is more than 1 match, otherwise all output is meaningless
      return loc > 0 ? new ArrayList<>(nowStep.values()) : new ArrayList<>();
    }

    /**
     * Fst Tree node traversal method
     *
     * @param returnSource All potential paths
     * @param buffer cumulative words
     * @param now current FST node
     */
    private void getDFSWord(List<WordWithFrequency> returnSource, String buffer, TreeNode now) {
      if (now.getNext().size() == 0 || now.isStop()) {
        returnSource.add(new WordWithFrequency(buffer + now.getStep(), now.getCount()));
      } else {
        now.getNext()
            .values()
            .forEach(each -> this.getDFSWord(returnSource, buffer + now.getStep(), each));
      }
    }

    private SortedSet<WordWithFrequency> tryComplicate(
        String word, TreeNode tree, SqlCompleteFstTree.Single passLength) {
      List<WordWithFrequency> temp = new ArrayList<>();
      List<WordWithFrequency> tempNPreview = new ArrayList<>();
      SortedSet<WordWithFrequency> returnSource;
      SqlCompleteFstTree.Single breLoc = new Single();
      this.getMaybeNodeList(word, tree, breLoc).forEach(each -> this.getDFSWord(temp, "", each));
      returnSource =
          temp.stream()
              .map(e -> new WordWithFrequency(word.substring(0, breLoc.loc) + e.word, e.count))
              .collect(Collectors.toCollection(TreeSet::new));

      // When FST appears that the prefix cannot be completely matched, such as: sela users may want
      // to enter sele, there is no sela in FstTree.
      // Try to correct errors. The error correction logic is to search for legal prefixes, that is,
      // search for sel.
      // Considering that the user enters the last word, although it is matched, it may be wrong.
      // When there is an illegal match, remove a word and try to get more recalls.
      // e.g: from, fre are prefixes that can be matched. If the last input is wrong, you can get
      // the correct prompt. (Similar search to increase recall and get more target values)

      if (breLoc.loc < word.length() && breLoc.loc > 1) {
        this.getMaybeNodeList(word.substring(0, breLoc.loc - 1), tree, breLoc)
            .forEach(each -> this.getDFSWord(tempNPreview, "", each));

        // Note: that due to the use of variable variables, here breloc has been-1
        returnSource.addAll(
            tempNPreview.stream()
                .map(e -> new WordWithFrequency(word.substring(0, breLoc.loc) + e.word, e.count))
                .collect(Collectors.toList()));
      }

      // returns the length of the last successful match, which is used to measure the correctness
      // of the match last time
      passLength.loc = breLoc.loc;
      return returnSource;
    }

    /**
     *
     *
     * <pre>
     * Return possible words with prefix matching and simple error correction function. The returned words conform to the following rules:
     *  - Without considering error correction
     *  1. The input can match the prefix completely; such as: 'selec', return such as: select 'selexxx'; select belongs to the high-frequency word ranked first
     *  2. Enter s to return all potential words beginning with s. High-frequency words are ranked first
     * <p>
     * - The definition of error (the premise that can be corrected is the preorder, and the reverse order must have a correct starting value)
     *  1. It is assumed that the first word entered by the user is correct, such as 'seleot', 'soloct', 'so', which can correct errors.
     *  2. For aelect, eelect, aect, oeleot, the follow-up is a correct starting point and can be corrected.
     *  3. For oelece, it is impossible to correct errors.
     * <p>
     * - How to correct
     *  1. Preorder, reverse order traversal, the result of the difference set plus + the maximum possible prefix match + the maximum possible prefix match -1 matching result
     * </pre>
     *
     * @param word need to be associated or corrected Words
     * @return return a potential word
     */
    public List<String> getComplicate(String word) {

      word = word.toLowerCase();
      SqlCompleteFstTree.Single searchFromHeadPassLength = new Single();
      SqlCompleteFstTree.Single searchFromReversePassLength = new Single();

      SortedSet<WordWithFrequency> head =
          tryComplicate(word, READ_FROM_HEAD, searchFromHeadPassLength);

      // Reverse search is used for error correction. Normal scenes have no meaning, such as sel,
      // the reverse order will return l data in reverse order (because there is no les character)
      SortedSet<WordWithFrequency> tail =
          tryComplicate(
                  new StringBuffer(word).reverse().toString(),
                  READ_FROM_HEAD,
                  searchFromReversePassLength)
              .stream()
              .map(
                  e ->
                      new WordWithFrequency(new StringBuffer(e.word).reverse().toString(), e.count))
              .collect(Collectors.toCollection(TreeSet::new));

      SortedSet<WordWithFrequency> temp = new TreeSet<>(head);
      temp.retainAll(tail);

      SortedSet<WordWithFrequency> returnSource = new TreeSet<>(temp);

      // Compare and return the matched length to prevent the previous input error and lead to error
      // correction, such as "aelect",
      // it should not be associated with "axxx", it should be corrected according to the subsequent
      // "elect"
      if (searchFromReversePassLength.loc > searchFromHeadPassLength.loc) {
        returnSource.addAll(tail);
      }

      returnSource.addAll(head);

      return returnSource.stream().map(e -> e.word).collect(Collectors.toList());
    }
  }

  private static class TreeNode {

    private final Character step;
    private boolean stop = false;
    private int count = 0;
    private final Map<Character, TreeNode> next;

    /**
     * The root node must be initialized to ' ', otherwise there can only be one character as the
     * starting node.
     *
     * @param step the character of the current node
     */
    public TreeNode(Character step) {
      this.step = step;
      this.next = new HashMap<>();
    }

    public void setCount(int count) {
      this.count += count;
    }

    public int getCount() {
      return count;
    }

    public void setStop() {
      this.stop = true;
    }

    public boolean isStop() {
      return stop;
    }

    public Character getStep() {
      return step;
    }

    public Map<Character, TreeNode> getNext() {
      return next;
    }
  }
}
