#!/bin/bash

data="/data/corpora/cqpweb/corpora/bncxml_old"
regfile="bncxml_old"
name="BNCXML_OLD"
registry="/data/corpora/cqpweb/registry"
infile="/data/treebank.info/upload/bncxml_old.xml"

export CORPUS_REGISTRY="$registry"

echo "cwb-encode"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -c utf8 -P lower -P pos -P lemma -P wc -P indep/ -P out_abbrev/ -P out_acomp/ -P out_advcl/ -P out_advmod/ -P out_agent/ -P out_amod/ -P out_appos/ -P out_attr/ -P out_aux/ -P out_auxpass/ -P out_cc/ -P out_ccomp/ -P out_complm/ -P out_conj/ -P out_cop/ -P out_csubj/ -P out_csubjpass/ -P out_dep/ -P out_det/ -P out_dobj/ -P out_expl/ -P out_infmod/ -P out_iobj/ -P out_mark/ -P out_mwe/ -P out_neg/ -P out_nn/ -P out_npadvmod/ -P out_nsubj/ -P out_nsubjpass/ -P out_num/ -P out_number/ -P out_parataxis/ -P out_partmod/ -P out_pcomp/ -P out_pobj/ -P out_poss/ -P out_possessive/ -P out_preconj/ -P out_predet/ -P out_prep/ -P out_prepc/ -P out_prt/ -P out_punct/ -P out_purpcl/ -P out_quantmod/ -P out_rcmod/ -P out_rel/ -P out_tmod/ -P out_xcomp/ -P out_xsubj -P root -S s:0+id+text+id_md5_base64+alternative_id -S text:0+id+alternative_id -0 corpus

echo "parallel run of cwb-make"

parallel -j 8 cwb-make -M 500 -V "$name" ::: word lower pos lemma wc indep out_abbrev out_acomp out_advcl out_advmod out_agent out_amod out_appos out_attr out_aux out_auxpass out_cc out_ccomp out_complm out_conj out_cop out_csubj out_csubjpass out_dep out_det out_dobj out_expl out_infmod out_iobj out_mark out_mwe out_neg out_nn out_npadvmod out_nsubj out_nsubjpass out_num out_number out_parataxis out_partmod out_pcomp out_pobj out_poss out_possessive out_preconj out_predet out_prep out_prepc out_prt out_punct out_purpcl out_quantmod out_rcmod out_rel out_tmod out_xcomp out_xsubj root

echo "cwb-make on remaining attributes"

cwb-make -M 500 -V "$name"
