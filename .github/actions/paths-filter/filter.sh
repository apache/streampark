#!/bin/bash

# ===================================================================
# Paths Filter - Multi-rule file change detector
# Replacement for dorny/paths-filter for GitHub Enterprise environments
# Supports: *, **, ?, !pattern, PR/push detection
# ===================================================================

get_changed_files() {
  local base head
  if [ "${GITHUB_EVENT_NAME}" = "pull_request" ]; then
    base=${{ github.event.pull_request.base.sha }}
    head=${{ github.event.pull_request.head.sha }}
  else
    base=${{ github.event.before }}
    head=${GITHUB_SHA}
    if [ "$base" = "0000000000000000000000000000000000000000" ] || [ -z "$base" ]; then
      base="HEAD~1"
    fi
  fi

  echo "::debug::Fetching base=$base, head=$head"
  git fetch --depth=1 origin "$base" || git fetch --depth=1 --no-tags "$base"
  git fetch --depth=1 origin "$head" || git fetch --depth=1 --no-tags "$head"

  git diff --name-only "$base" "$head" || echo ""
}

match_pattern() {
  local file=$1
  local pattern=$2
  local negate=false

  [[ $pattern == !* ]] && negate=true && pattern="${pattern#!}"

  local regex="^"
  local i c
  for (( i=0; i<${#pattern}; i++ )); do
    c="${pattern:$i:1}"
    case "$c" in
      "*") regex+=".*" ;;
      "?") regex+="." ;;
      ".") regex+="\." ;;
      "/") regex+="/?" ;;
      *)   regex+="$c" ;;
    esac
  done
  regex+="$"

  [[ $file =~ $regex ]] && result=true || result=false

  if [ "$negate" = "true" ]; then
    [ "$result" = "false" ]
  else
    [ "$result" = "true" ]
  fi
}

main() {
  local filters="$INPUT_FILTERS"
  local changed_files
  mapfile -t changed_files < <(get_changed_files)

  echo "📄 Found $((${#changed_files[@]})) changed files"
  for file in "${changed_files[@]}"; do
    [ -n "$file" ] && echo "  $file"
  done

  local current_group=""
  local lines
  mapfile -t lines <<< "$filters"

  declare -A group_match

  for line in "${lines[@]}"; do
    [[ -z "$line" ]] && continue

    if [[ $line =~ ^[[:space:]]*([a-zA-Z0-9_-]+):[[:space:]]*$ ]]; then
      current_group="${BASH_REMATCH[1]}"
      group_match["$current_group"]=false
    elif [[ $line =~ ^[[:space:]]*-[[:space:]]*[\'\"]?(.*[^\'\"])[\'\"]?[[:space:]]*$ ]]; then
      local pattern="${BASH_REMATCH[1]}"
      pattern="${pattern%\"}" && pattern="${pattern%\'}"
      pattern="${pattern#\"}" && pattern="${pattern#\'}"

      for file in "${changed_files[@]}"; do
        if [ -n "$file" ] && match_pattern "$file" "$pattern"; then
          group_match["$current_group"]=true
          echo "::debug::Match: '$file' ~ '$pattern' => $current_group=true"
        fi
      done
    fi
  done

  for group in "${!group_match[@]}"; do
    echo "$group=${group_match[$group]}" >> $GITHUB_OUTPUT
    echo "::debug::Output: $group=${group_match[$group]}"
  done
}

main "$@"
