#!/usr/bin/env bash
set -e

CURRENT_BRANCH=$(git branch --show-current)

if [[ "$CURRENT_BRANCH" == "main" || "$CURRENT_BRANCH" == "master" ]]; then
    echo "ERROR: Working on protected branch ($CURRENT_BRANCH)"
    exit 1
fi

echo "Branch validation OK: $CURRENT_BRANCH"
