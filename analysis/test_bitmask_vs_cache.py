#!/usr/bin/env python3

# We will first run a bitmask operation trial number of times
# We will then run a function where we first pre-calculate bitmasks. Then we repeatedly read from cache trial number of times.

# The cache approach was consistently faster, although both are blazing fast.
# Funnily enough, kernprof indicated that the bitmask operation was faster than the cache approach for some reason...

import sys
import datetime
import random

if sys.version_info[0] == 2:
    py_ver = 2
else:
    py_ver = 3

# No-op version of @profile, so that we don't need to keep uncommenting @profile when we're not using kernprof
try:
    # Python 2
    import __builtin__ as builtins
except ImportError:
    # Python 3
    import builtins

try:
    builtins.profile
except AttributeError:
    # No line profiler, provide a pass-through version
    def profile(func): return func
    builtins.profile = profile

@profile
def test_bitmask():

    for num in rand_nums:
        mask = 1<<num

@profile
def test_cache():

    cache = {}
    for i in range(256):
        cache[i] = 1<<i

    for num in rand_nums:
        mask = cache[num]

@profile
def main():

    for i in range(4000000):        
        rand_nums.append(random.randint(0, 255) )

    start_time = datetime.datetime.now()
    sys.stderr.write("\nStarted at: {0}\n".format(str(start_time) ) )
    test_bitmask()
    bitmask_done_time = datetime.datetime.now()
    test_cache()
    cache_done_time = datetime.datetime.now()
    sys.stderr.write("\nFinished testing bitmask at: {0}\n".format(str(bitmask_done_time) ) )
    sys.stderr.write("\nFinished testing cache at: {0}\n".format(str(cache_done_time) ) )
    sys.stderr.write("\nBitmask calc time: {0}\n".format(bitmask_done_time - start_time) )
    sys.stderr.write("\nCache lookup time: {0}\n".format(cache_done_time - bitmask_done_time) )

rand_nums = []
main()
