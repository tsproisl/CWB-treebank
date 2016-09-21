#!/bin/bash

data="/var/treebank.info/corpora/c4ed3d546e3fdac186a000000"
regfile="c4ed3d546e3fdac186a000000"
name="C4ED3D546E3FDAC186A000000"
registry="/var/treebank.info/registry"
infile="/home/pandora/4ed3d546e3fdac186a000000.xml"

export CORPUS_REGISTRY="$registry"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -P pos -P lemma -P wc -P indep -P outdep -P root -S s:0+id+id_md5_base64+original_id -S text:0+id+original_id -0 corpus

cwb-make -M 500 -V "$name"
