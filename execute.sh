#!/bin/bash
CLASS=$1 
shift
spark-submit --class $CLASS --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar $@
