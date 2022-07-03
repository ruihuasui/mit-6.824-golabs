#!/usr/bin/env bash
GOLABROOT=$(cd "$(dirname $0)/.." && pwd)
source ${GOLABROOT}/scripts/env.sh

cd ${GOLABROOT}/src/main

$go13 build -buildmode=plugin ../mrapps/wc.go

rm mr-out*
# $go13 run mrsequential.go wc.so pg*.txt
$go13 run mrmaster.go pg-*.txt
more mr-out-0