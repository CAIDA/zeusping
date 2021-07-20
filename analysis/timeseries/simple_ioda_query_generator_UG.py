
#  This software is Copyright (c) 2021 The Regents of the University of
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

import sys

reqd_asns = {
    # '20294' : 'MTN',
    '29039' : 'Africa Online Uganda',
    # '36991' : 'Africell Uganda',
    # '36997' : 'Infocom',
    # '37063' : 'Roke',
    '37075' : 'Airtel Uganda',
    '36977' : 'Airtel Uganda',
    # '21491' : 'Uganda Telecom',
    # '328015' : 'Sombha',
    # '328198' : 'Blue Crane',
    # '37122' : 'Smile',
    # '37113' : 'Tangerine',
    # '327687' : 'RENU',
}

sig = sys.argv[1]
query = ''
for asn, asname in reqd_asns.items():

    if sig == 'active':
        this_query = '''
        alias(
          removeEmpty(
            normalize(
              sumSeries(
                keepLastValue(
                  active.ping-slash24.asn.{0}.probers.team-1.caida-sdsc.*.up_slash24_cnt,
                  1
                )
              )
            )
          ),
          "{1} (AS{0})"
        )'''.format(asn, asname)
    elif sig == 'bgp':
        this_query = '''
        alias(
            normalize(
             bgp.prefix-visibility.asn.{0}.v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt
            ),
          "{1} (AS{0})"
        )'''.format(asn, asname)

    query += '{0},'.format(this_query)


sys.stdout.write("{0}\n".format(query[:-1]) )
