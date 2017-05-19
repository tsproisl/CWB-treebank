#!/bin/bash

data="/data/corpora/cqpweb/corpora/sascitex_old"
regfile="sascitex_old"
name="SASCITEX_OLD"
registry="/data/corpora/cqpweb/registry"
infile="/data/treebank.info/upload/sascitex_old.xml"

export CORPUS_REGISTRY="$registry"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -c utf8 -P lower -P pos -P lemma -P wc -P indep/ -P abbrev/ -P acomp/ -P advcl/ -P advmod/ -P agent/ -P amod/ -P appos/ -P attr/ -P aux/ -P auxpass/ -P cc/ -P ccomp/ -P complm/ -P conj/ -P cop/ -P csubj/ -P csubjpass/ -P dep/ -P det/ -P dobj/ -P expl/ -P infmod/ -P iobj/ -P mark/ -P mwe/ -P neg/ -P nn/ -P npadvmod/ -P nsubj/ -P nsubjpass/ -P num/ -P number/ -P parataxis/ -P partmod/ -P pcomp/ -P pobj/ -P poss/ -P possessive/ -P preconj/ -P predet/ -P prep/ -P prepc/ -P prt/ -P punct/ -P purpcl/ -P quantmod/ -P rcmod/ -P rel/ -P tmod/ -P xcomp/ -P xsubj/ -P root -S s:0+id+text+id_md5_base64+alternative_id -S text:0+id+alternative_id -0 corpus

parallel -j 8 cwb-make -M 500 -V "$name" ::: word lower pos lemma wc indep abbrev acomp advcl advmod agent amod appos attr aux auxpass cc ccomp complm conj cop csubj csubjpass dep det dobj expl infmod iobj mark mwe neg nn npadvmod nsubj nsubjpass num number parataxis partmod pcomp pobj poss possessive preconj predet prep prepc prt punct purpcl quantmod rcmod rel tmod xcomp xsubj root

cwb-make -M 500 -V "$name"
