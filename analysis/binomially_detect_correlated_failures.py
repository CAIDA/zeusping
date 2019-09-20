
# TODO: Think about how to integrate new resps!

import sys
import math
import numpy
import scipy
from scipy.stats import binom
import datetime

conf = 0.9999
reqd_conf = float(conf)

processed_op_dir = '/fs/nm-thunderping/weather_alert_prober_logs_master_copy/zeusping/data_from_aws/processed_op_randsorted_colorado_4M/'
resp_dropout_per_round_fname = '{0}/responsive_and_dropout_addrs/resp_dropout_per_round'.format(processed_op_dir)
resp_dropout_per_round_fp = open(resp_dropout_per_round_fname, 'r')

total_n_r = 0
total_n_d = 0

line_ct = 0
for line in resp_dropout_per_round_fp:

    line_ct += 1

    if line_ct < 200:
        continue

    parts = line.strip().split()
    n_r = int(parts[1])
    n_d = int(parts[2])
    n_n = int(parts[3])
    total_n_r += n_r
    total_n_d += n_d

p_d = float(total_n_d)/total_n_r

p_d *= 2

sys.stderr.write("Probability of dropout is: {0}\n".format(p_d) )

resp_dropout_per_round_fp.close()
resp_dropout_per_round_fp = open(resp_dropout_per_round_fname, 'r')

line_ct = 0
for line in resp_dropout_per_round_fp:

    line_ct += 1

    if line_ct%100 == 0:
        sys.stderr.write("Processed {0} lines at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )

    parts = line.strip().split()
    round_tstamp = int(parts[0])
    n_r = int(parts[1])
    n_d = int(parts[2])

    if (n_d > 1): # A single outage cannot be a correlated failure!
        
        max_necessary = float(n_r*0.65) # Even with a 0.5 P(f), no need to calculate beyond 0.65 * N        
        k = numpy.arange(max_necessary)
    
        binomial_cdf = scipy.stats.binom.cdf(k, n_r, p_d)

        min_outs_reqd = 0
        for elem in binomial_cdf:
            # if elem > 0.9999:
            if elem > reqd_conf:
            # if elem > 0.99:
            # if elem > 0.95:
                # print nups, nouts, min_outs_reqd, elem
                break
            min_outs_reqd += 1

        # At this point, the probability of min_outs_reqd or fewer failures is < 1%. Therefore, the minimum number of outages should be *greater* than min_outs_reqd, for there to be l.t. 1% probability that so many failures happened independently
        min_outs_reqd += 1

        if (n_d >= min_outs_reqd):
            # outs_sf = scipy.stats.binom.sf(nouts-1, nups, fprob)
            # Calc the cdf of prob that that we saw nouts-1 failures. 1 - cdf is the prob that we saw nouts (or g.t. nouts) failures.
            outs_cdf = scipy.stats.binom.cdf(n_d-1, n_r, p_d)
            
            sys.stdout.write("{0} {1} {2} {3} {4}\n".format(round_tstamp, n_d, n_r, min_outs_reqd, 1 - outs_cdf) )


            
