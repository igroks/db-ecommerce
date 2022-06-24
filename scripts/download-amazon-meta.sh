#!/bin/bash
if ! [ -d ./resources ]; then
     echo "Não tem pasta"
fi 
if ! [ -f ./resources/amazon-meta.txt ]; then
    echo "Não tem arquivo"
fi
