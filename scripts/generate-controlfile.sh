#!/usr/bin/env bash

toolspec=$1
operation=$2
inputdir=$3
outputdir=$4
outputext=$5
controlfile=$6

if [ -z "$inputdir" -o  \
     -z "$outputdir" -o \
     -z "$toolspec" -o \
     -z "$operation" -o \
     -z "$outputext" -o \
     -z "$controlfile" ]; then
    echo "Generates a controlfile for ToMaR with an input and an output file parameter"
    echo "Usage: $0 <toolspec> <operation> <input-dir-on-hdfs> <output-dir-on-hdfs> <output-file-extension> <controlfile>"
    exit 1
fi

homedir=hdfs:///user/$USER/

for i in `hdfs dfs -ls $inputdir | awk '/hadoop/ {print $8}'`; do
    o=`echo $i | sed -e "s,$inputdir,,g"`
    echo "$toolspec $operation --input=\"$homedir$i\" --output=\"$homedir$outputdir$o.$outputext\"" >> $controlfile
done
