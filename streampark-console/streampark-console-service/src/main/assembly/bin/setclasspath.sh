#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# -----------------------------------------------------------------------------
#  Set JAVA_HOME or JRE_HOME if not already set, ensure any provided settings
#  are valid and consistent with the selected start-up options.
# -----------------------------------------------------------------------------

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
mingw=false
case "$(uname)" in
  CYGWIN*) cygwin=true ;;
  MINGW*) mingw=true;;
  Darwin*) darwin=true
    # Use /usr/libexec/java_home if available, otherwise fall back to /Library/Java/Home
    # See https://developer.apple.com/library/mac/qa/qa1170/_index.html
    if [[ -z "$JAVA_HOME" ]]; then
      if [[ -x "/usr/libexec/java_home" ]]; then
        JAVA_HOME="$(/usr/libexec/java_home)"; export JAVA_HOME
      else
        JAVA_HOME="/Library/Java/Home"; export JAVA_HOME
      fi
    fi
    ;;
esac

if [[ -z "$JAVA_HOME" ]]; then
  if [[ -r /etc/gentoo-release ]]; then
    JAVA_HOME=$(java-config --jre-home)
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] &&
    CLASSPATH=$(cygpath --path --unix "$CLASSPATH")
fi

# For Mingw, ensure paths are in UNIX format before anything is touched
if $mingw ; then
  [ -n "$JAVA_HOME" ] && [ -d "$JAVA_HOME" ] &&
    JAVA_HOME="$(cd "$JAVA_HOME" || (echo "cannot cd into $JAVA_HOME."; exit 1); pwd)"
fi

if [[ -z "$JAVA_HOME" ]]; then
  javaExecutable="$(which javac)"
  if [[ -n "$javaExecutable" ]] && ! [[ "$(expr "\"$javaExecutable\"" : '\([^ ]*\)')" = "no" ]]; then
    # readlink(1) is not available as standard on Solaris 10.
    readLink=$(which readlink)
    if [[ ! "$(expr "$readLink" : '\([^ ]*\)')" = "no" ]]; then
      if $darwin ; then
        javaHome="$(dirname "\"$javaExecutable\"")"
        javaExecutable="$(cd "\"$javaHome\"" && pwd -P)/javac"
      else
        javaExecutable="$(readlink -f "\"$javaExecutable\"")"
      fi
      javaHome="$(dirname "\"$javaExecutable\"")"
      javaHome=$(expr "$javaHome" : '\(.*\)/bin')
      JAVA_HOME="$javaHome"
      export JAVA_HOME
    fi
  fi
fi

if [[ -z "$JAVACMD" ]]; then
  if [[ -n "$JAVA_HOME"  ]]; then
    if [[ -x "$JAVA_HOME/jre/sh/java" ]]; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD="$(\unset -f command 2>/dev/null; \command -v java)"
  fi
fi

if [[ ! -x "$JAVACMD" ]]; then
  echo "Error: JAVA_HOME is not defined correctly." >&2
  echo "  We cannot execute $JAVACMD" >&2
  exit 1
fi

if [[ -z "$JAVA_HOME" ]]; then
  echo "Warning: JAVA_HOME environment variable is not set."
fi

REQUIRED_JAVA_MAJOR=11
# shellcheck disable=SC2006
java_version=`"$JAVACMD" -version 2>&1 | awk -F '"' '/version/ {print $2}'`
java_major=$(echo "$java_version" | awk -F '.' '{if ($1 == 1) {print $2} else {print $1}}')
if [[ "$java_major" -lt "$REQUIRED_JAVA_MAJOR" ]]; then
  echo "Error: StreamPark requires JDK ${REQUIRED_JAVA_MAJOR} or later (current: ${java_version})." >&2
  exit 1
fi
