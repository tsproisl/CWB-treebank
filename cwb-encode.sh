#!/bin/bash

data="/localhome/Corpora/BNC_PARSED"
regfile="bnc_parsed"
name="BNC_PARSED"
outdir=$5
registry="/localhome/Databases/CWB/registry"
infile="/localhome/Databases/BNC/bnc.xml.lem.deps"

export CORPUS_REGISTRY="$registry"
export PERL5LIB="/home/linguistik/tsproisl/local/lib/perl5/site_perl"

cwb-encode -d $data $infile -R "$registry/$regfile" -xsB -P pos -P lemma -P wc -P indep -P outdep -S s:0+id+dbid -0 corpus

cwb-make -M 500 -V "$name"
