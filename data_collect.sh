#!/bin/bash

dir=$(ls data)

echo $dir

for folder in $dir
do
	cd data/$folder
	cp * ../../rawdata
	cd ../..
done