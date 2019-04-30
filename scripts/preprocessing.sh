#!/bin/bash

for e in $(ls -l ../raw | grep '^d' | awk '{ print $9 }')
do
	python3 ../code/Preprocessing.py ../raw/$e
done