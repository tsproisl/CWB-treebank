#!/bin/bash

data="/data/corpora/cqpweb/corpora/taz2010bis2011_old"
regfile="taz2010bis2011_old"
name="TAZ2010BIS2011_OLD"
registry="/data/corpora/cqpweb/registry"
infile="/data/treebank.info/upload/taz2010bis2011_old.xml"

export CORPUS_REGISTRY="$registry"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -c utf8 -P lower -P pos -P lemma -P wc -P indep/ -P AC/ -P ADC/ -P AG/ -P AMS/ -P APP/ -P AVC/ -P CC/ -P CD/ -P CJ/ -P CM/ -P CP/ -P CVC/ -P DA/ -P DH/ -P DM/ -P EP/ -P JU/ -P MNR/ -P MO/ -P NG/ -P NK/ -P NMC/ -P OA/ -P OA2/ -P OC/ -P OG/ -P OP/ -P PAR/ -P PD/ -P PG/ -P PH/ -P PM/ -P PNC/ -P RC/ -P RE/ -P RS/ -P SB/ -P SBP/ -P SP/ -P SVP/ -P UC/ -P VO/ -P root -S s:0+id+text+id_md5_base64+alternative_id -S text:0+id+alternative_id -0 corpus

parallel -j 8 cwb-make -M 500 -V "$name" ::: word lower pos lemma wc indep AC ADC AG AMS APP AVC CC CD CJ CM CP CVC DA DH DM EP JU MNR MO NG NK NMC OA OA2 OC OG OP PAR PD PG PH PM PNC RC RE RS SB SBP SP SVP UC VO root

cwb-make -M 500 -V "$name"
