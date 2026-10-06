#!/usr/bin/env bash
set -e

while getopts d: param
do
    case "${param}" in
        d) PROJECT_DIRECTORY=${OPTARG};
    esac
done

PROMPT_DIRECTORY="$PROJECT_DIRECTORY/.goose/prompts"
KEEP_DIR_FILE=.keep_dir

mkdir -p "$PROJECT_DIRECTORY/.goose/backlog"
touch "$PROJECT_DIRECTORY/.goose/backlog/$KEEP_DIR_FILE"

mkdir -p $PROMPT_DIRECTORY
cd $PROMPT_DIRECTORY

mkdir -p \
    to_close \
    to_do_doc_generate \
    to_do_feature_build \
    to_do_feature_plan \
    to_do_feature_validate \
    to_do_review \
    to_do_review_fix \
    to_do_test_generate \
    to_merge

touch \
    "to_close/$KEEP_DIR_FILE" \
    "to_do_doc_generate/$KEEP_DIR_FILE" \
    "to_do_feature_build/$KEEP_DIR_FILE" \
    "to_do_feature_plan/$KEEP_DIR_FILE" \
    "to_do_feature_validate/$KEEP_DIR_FILE" \
    "to_do_review/$KEEP_DIR_FILE" \
    "to_do_review_fix/$KEEP_DIR_FILE" \
    "to_do_test_generate/$KEEP_DIR_FILE" \
    "to_merge/$KEEP_DIR_FILE" \
