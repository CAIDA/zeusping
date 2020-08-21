#!/bin/bash 

file=$1
wandiocat $file | sc_warts2json
