

import sys
import glob
import shlex
import subprocess32
import subprocess
import os
import datetime
import json

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('spark_test_wc')
sc = SparkContext(conf=conf)

def wc():
    
    

def analyze():
    list_of_files = []
    for i in range(4):
        list_of_files.append('temp{0}'.format(i) )

    ls_rdd = sc.parallelize(list_of_files)

    raw_results = ls_rdd.flatMap(wc)

    
def main():
    analyze()

    
if __name__ == "__main__":
    main()
