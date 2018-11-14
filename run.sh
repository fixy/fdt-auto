#!/bin/sh
while true;
do
 echo 'Start NEW Run'
 python NetTester.py $1
 echo 'Sleeping 20 seconds'
 date
 sleep 20
done
