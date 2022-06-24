#!/bin/bash 
if ! [ -f /resources/amazon-meta.txt]; then
    wget https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz -O resources/amazon-meta.txt.gz
    gunzip resources/amazon-meta.txt.gz
fi
