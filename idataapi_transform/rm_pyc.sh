#!/bin/bash
#Autohr:Icey.D
#Version:1.0.0
#Command format: remove *.pyc

for del in $(find "." -type f -name "*.pyc")
do
    if [ ${del##*.}=="pyc" ];then
        rm ${del}
		echo "["`date '+%Y-%m-%d %H:%M:%S'`"]DeleteInfo: File ${del} is deleted!"
	fi
done