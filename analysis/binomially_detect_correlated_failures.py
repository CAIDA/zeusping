
#  This software is Copyright (c) 2019 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

# TODO: Think about how to integrate new resps!

import sys
import math
import numpy
import scipy
from scipy.stats import binom
import datetime

conf = 0.9999
reqd_conf = float(conf)

rda_fname = sys.argv[1]
rda_fp = open(rda_fname)

total_n_r = 0
total_n_d = 0

line_ct = 0
for line in rda_fp:

    line_ct += 1

    # if line_ct < 200:
    #     continue

    if line_ct == 1:
        continue

    parts = line.strip().split()
    n_d = int(parts[1])
    n_r = int(parts[2])
    n_a = int(parts[3])
    total_n_r += n_r
    total_n_d += n_d

# Let us ensure statistical significance    
sample_prop = total_n_d/float(total_n_r)
sample_prop_complement = 1 - sample_prop
thresh = total_n_r * sample_prop * sample_prop_complement


sys.stderr.write("Threshold is {0:.4f}\n".format(thresh) )

if ( (thresh < 10) ):
    sys.stderr.write("Threshold is too small, not enough samples\n")
    sys.exit(1)

sys.stderr.write("Probability of dropout is: {0:.6f}\n".format(sample_prop) )    
p_d = sample_prop

p_d *= 2

sys.stderr.write("Probability of dropout is: {0}\n".format(p_d) )

rda_fp.close()
rda_fp = open(rda_fname, 'r')

line_ct = 0
for line in rda_fp:

    line_ct += 1

    if line_ct%100 == 0:
        sys.stderr.write("Processed {0} lines at {1}\n".format(line_ct, str(datetime.datetime.now() ) ) )

    parts = line.strip().split()
    round_tstamp = int(parts[0])
    n_d = int(parts[1])
    n_r = int(parts[2])

    if (n_d > 1): # A single outage cannot be a correlated failure!

        # TODO: Consider moving the following two steps outside. We can approximate how much n_r is in each round and calculate these a single time. 
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


rda_fp.close()
