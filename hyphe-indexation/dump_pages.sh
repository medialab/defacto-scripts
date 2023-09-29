#!/bin/bash

source config.inc

minet hyphe dump "$HYPHE_API" "$HYPHE_CORPUS" -O "$PAGES_DIR" --statuses IN -o "dump-hyphe-pages.log"

