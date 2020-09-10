
import sys
import glob
import shlex
import subprocess32
import subprocess
import os
import datetime
import json
import numpy as np

from pyspark import SparkContext, SparkConf

sc=SparkContext()
lst=np.random.randint(0,10,20)
A=sc.parallelize(lst)
print "\n\n\n"
# print type(A)

# A.collect()
print A.glom().collect()
print "\n\n\n"
