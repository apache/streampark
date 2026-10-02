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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the declared name and {@code WITH (...)} options from one {@code CREATE
 * [TEMPORARY] TABLE} or {@code CREATE CATALOG} statement.
 *
 * <p>Why this exists: a Flink {@code CompiledPlan} gives an accurate {@code
 * `catalog`.`database`.`table`} identifier only for tables that live in a real, attached Flink
 * catalog (e.g. a Paimon catalog registered via {@code CREATE CATALOG}). For a table created
 * per-job via {@code CREATE TABLE ... WITH (...)} — the common shape for StreamPark Flink SQL jobs,
 * which are typically self-contained scripts with no attached catalog — the plan JSON reports only
 * "session default catalog/database + local table name", carrying no connector/host/physical-table
 * info at all. That information lives in the SQL text itself, so it is extracted here instead.
 *
 * <p>The same holds one level up for {@code CREATE CATALOG ... WITH ('type' = '...')} — see
 * {@link CompiledPlanLineageParser#resolveOne} for why a catalog's declared type has to come from
 * the SQL text as well.
 *
 * <p>Deliberately not scoped to {@code TEMPORARY} tables only (unlike the reference implementation
 * this was ported from): a plain {@code CREATE TABLE} without an attached persistent catalog is
 * exactly as ephemeral as a {@code CREATE TEMPORARY TABLE} from Gravitino's point of view, and
 * StreamPark SQL jobs commonly omit the {@code TEMPORARY} keyword. Restricting to {@code TEMPORARY}
 * would silently lose lineage for the common case.
 */
public final class SqlWithOptionsParser {

    /**
     * The alternatives of a single SQL identifier, bare or backtick-quoted (a quoted one may contain
     * dots). Deliberately ungrouped, so each use can wrap it in whichever kind of group it needs
     * without nesting a redundant one inside. Both letter cases are spelled out, so the pattern
     * using it must not be compiled {@code CASE_INSENSITIVE} — only the keyword patterns are, which
     * also keeps identifier matching case-exact, as Flink treats it.
     */
    private static final String IDENTIFIER = "`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*";

    /** The {@code CREATE ... TABLE} keywords, up to where the declared name starts. */
    private static final Pattern CREATE_TABLE_KEYWORDS =
        Pattern.compile(
            "\\s*CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?",
            Pattern.CASE_INSENSITIVE);

    /** The {@code CREATE CATALOG} keywords, up to where the declared name starts. */
    private static final Pattern CREATE_CATALOG_KEYWORDS =
        Pattern.compile("\\s*CREATE\\s+CATALOG\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?", Pattern.CASE_INSENSITIVE);

    /**
     * The declared name, matched from just past those keywords. It may be qualified ({@code CREATE
     * TABLE mydb.mytable ...}); the qualifier prefix is matched but not captured, so the capture
     * group is the local name alone — that is what a {@code CompiledPlan} identifier reports the
     * table under. Kept apart from the keywords rather than inlined into both patterns above: one
     * definition of what a declared name looks like, and neither pattern then has to carry the
     * other's share of the complexity.
     *
     * <p>The qualifier repetition is possessive: every iteration ends in a dot, so no match ever
     * needs to backtrack into an already-accepted qualifier, and a possessive group repetition is
     * matched iteratively rather than recursively (no stack growth proportional to the identifier
     * count).
     */
    private static final Pattern DECLARED_NAME =
        Pattern.compile("(?:(?:" + IDENTIFIER + ")\\s*\\.\\s*)*+(" + IDENTIFIER + ")");

    private static final Pattern WITH_CLAUSE = Pattern.compile("\\bWITH\\s*\\(", Pattern.CASE_INSENSITIVE);
    /**
     * {@code 'key' = 'value'}. A literal's embedded {@code ''} must count as an escaped single quote,
     * not the end of the literal — a connector option value (e.g. a password) may legitimately
     * contain a quote escaped this way, and treating it as the literal's end would truncate the
     * value at that point.
     *
     * <p>Both literal bodies repeat possessively. Nothing inside a literal can match the closing
     * quote that follows it (a {@code '} is only ever consumed as part of {@code ''} or {@code \'}),
     * so a well-formed entry never needs to give characters back; only an unterminated literal now
     * fails to match instead of being salvaged into a truncated option, which is the better outcome
     * for a lenient parser. The gain is that a possessive group repetition is matched iteratively —
     * a greedy one recurses once per character, overflowing the stack on a long option value.
     */
    private static final Pattern OPTION_ENTRY =
        Pattern.compile("'((?:[^'\\\\]|\\\\.|'')*+)'\\s*=\\s*'((?:[^'\\\\]|\\\\.|'')*+)'");

    private SqlWithOptionsParser() {
    }

    /** One declaration's local (unqualified) name and its {@code WITH (...)} options. */
    public static final class WithOptions {

        private final String name;
        private final Map<String, String> options;

        WithOptions(String name, Map<String, String> options) {
            this.name = name;
            this.options = options;
        }

        public String name() {
            return name;
        }

        public Map<String, String> options() {
            return options;
        }
    }

    /**
     * Parses one {@code CREATE [TEMPORARY] TABLE} statement. Returns {@code null} (not an
     * exception — this is production submission-path code, not a validator) when the statement
     * does not match the expected shape or carries no {@code WITH (...)} clause, e.g. {@code CREATE
     * TABLE ... LIKE ...} or a catalog-backed table with no inline connector options.
     */
    public static WithOptions parse(String createTableStatement) {
        return parseDeclaration(CREATE_TABLE_KEYWORDS, createTableStatement);
    }

    /**
     * Parses one {@code CREATE CATALOG} statement, same contract as {@link #parse}. The interesting
     * option is {@code 'type'}, which names the catalog implementation (paimon, hive, jdbc, ...).
     */
    public static WithOptions parseCatalog(String createCatalogStatement) {
        return parseDeclaration(CREATE_CATALOG_KEYWORDS, createCatalogStatement);
    }

    /**
     * Records a {@code CREATE CATALOG}'s declared {@code 'type'} under its catalog name, or does
     * nothing when the statement declares none. Lives here rather than in each per-Flink-version
     * extractor that calls it: it is pure SQL-text parsing with no version-specific type in its
     * signature, so duplicating it alongside those extractors would only risk them diverging.
     */
    public static void rememberCatalogType(String createCatalogStatement, Map<String, String> catalogTypes) {
        WithOptions catalog = parseCatalog(createCatalogStatement);
        if (catalog == null) {
            return;
        }
        String type = catalog.options().get("type");
        if (type != null) {
            catalogTypes.put(catalog.name(), type);
        }
    }

    private static WithOptions parseDeclaration(Pattern keywords, String statement) {
        Matcher keywordMatcher = keywords.matcher(statement);
        if (!keywordMatcher.lookingAt()) {
            return null;
        }
        // Anchored at the keywords' end rather than searched for: a name found anywhere else in the
        // statement (a column, a WITH value) would not be the declared one.
        Matcher nameMatcher = DECLARED_NAME.matcher(statement).region(keywordMatcher.end(), statement.length());
        if (!nameMatcher.lookingAt()) {
            return null;
        }
        String name = unquote(nameMatcher.group(1));

        int bodyStart = findWithClauseBodyStart(statement);
        if (bodyStart < 0) {
            return null;
        }
        String body = extractParenthesizedBody(statement, bodyStart);
        if (body == null) {
            return null;
        }

        Map<String, String> options = new LinkedHashMap<>();
        Matcher entry = OPTION_ENTRY.matcher(body);
        while (entry.find()) {
            options.put(unescapeLiteral(entry.group(1)), unescapeLiteral(entry.group(2)));
        }
        return new WithOptions(name, options);
    }

    /** Strips the backticks around a quoted identifier: {@code `my.table`} to {@code my.table}. */
    private static String unquote(String identifier) {
        return identifier.startsWith("`") ? identifier.substring(1, identifier.length() - 1) : identifier;
    }

    /**
     * Index just past the opening {@code (} of the first {@code WITH (} that is not itself inside a
     * string literal, or {@code -1}. The quote check matters: a column {@code COMMENT 'see WITH
     * (x)'} would otherwise be mistaken for the options clause and parsed as garbage.
     */
    private static int findWithClauseBodyStart(String sql) {
        Matcher withStart = WITH_CLAUSE.matcher(sql);
        int scanned = 0;
        boolean inSingleQuote = false;
        while (withStart.find()) {
            while (scanned < withStart.start()) {
                if (sql.charAt(scanned) == '\'') {
                    inSingleQuote = !inSingleQuote;
                }
                scanned++;
            }
            if (!inSingleQuote) {
                return withStart.end();
            }
        }
        return -1;
    }

    /** Balanced-parenthesis scan from just past the opening {@code (}, quote-aware. */
    private static String extractParenthesizedBody(String sql, int bodyStart) {
        int depth = 1;
        int idx = bodyStart;
        boolean inSingleQuote = false;
        while (idx < sql.length() && depth > 0) {
            char c = sql.charAt(idx);
            if (c == '\'') {
                inSingleQuote = !inSingleQuote;
            } else if (!inSingleQuote && c == '(') {
                depth++;
            } else if (!inSingleQuote && c == ')') {
                depth--;
            }
            idx++;
        }
        if (depth != 0) {
            return null;
        }
        return sql.substring(bodyStart, idx - 1);
    }

    private static String unescapeLiteral(String literal) {
        return literal.replace("''", "'");
    }
}
