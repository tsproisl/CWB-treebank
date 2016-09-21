#!/bin/bash

data="/var/treebank.info/corpora/c505487317a2853de55000000"
regfile="c505487317a2853de55000000"
name="C505487317A2853DE55000000"
registry="/var/treebank.info/registry"
infile="/home/pandora/505487317a2853de55000000.xml"

export CORPUS_REGISTRY="$registry"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -P pos -P lemma -P wc -P indep -P outdep -P root -S s:0+id+id_md5_base64+original_id -S text:0+id+original_id -0 corpus

cwb-make -M 500 -V "$name"