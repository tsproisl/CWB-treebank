#!/bin/bash

if [ $# -ne 5 ]
then
    echo "./cwb-encode.sh data_directory corpus_name registry_directory registry_file input_file"
    exit -1
fi

data=$1
name=$2
registry=$3
regfile=$4
infile=$5

mkdir -p $data

export CORPUS_REGISTRY="$registry"

cwb-encode -d $data -f $infile -R "$registry/$regfile" -xsB -P pos -P lemma -P wc -P indep -P outdep -P root -S s:0+id+id_md5_base64+original_id -S text:0+id+original_id -0 corpus

cwb-make -M 500 -V "$name"
