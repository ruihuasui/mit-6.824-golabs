#!/usr/bin/env bash
GOLABROOT=$(cd "$(dirname $0)/.." && pwd)
source ${GOLABROOT}/scripts/env.sh

cd ${GOLABROOT}/src/main

$go13 build -buildmode=plugin ../mrapps/wc.go
$go13 run mrsequential.go wc.so pg*.txt
more mr-out-0