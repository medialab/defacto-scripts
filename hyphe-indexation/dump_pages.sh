#!/bin/bash

source config.inc

minet hyphe dump "$HYPHE_API" "$HYPHE_CORPUS" --body -O "$PAGES_DIR" --statuses IN -o "dump-hyphe-pages.log"

