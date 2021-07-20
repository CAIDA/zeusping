
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
import struct
import socket
import ctypes

mode = 'ITERUNPACK'

struct_fmt = struct.Struct("I 5H")
buf = ctypes.create_string_buffer(struct_fmt.size * 2000) # NOTE: I never ended up doing the experiment with buf and unpack_from... some day, perhaps, although I don't expect to receive huge gains in timing

# all_data = open(sys.argv[1], 'rb').read()
# all_data_fp = open(sys.argv[1], 'rb')
with open(sys.argv[1], 'rb') as all_data_fp:
    # this_val = all_data_fp.read()
    while True:
        if mode == 'SIMPLE': # Took 17s to process one round's worth of data
            data_chunk = all_data_fp.read(struct_fmt.size)

            # In python, once EoF is reached, we'll just read an emptry string (even for binary files), so check if chunk size is 0
            if len(data_chunk) == 0:
                break

            ipid, sent, succ, au, err, loss = struct_fmt.unpack(data_chunk)

            ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

            sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )
            
            data_chunk = ''

        elif mode == 'ITERUNPACK': # Took 13s to process one round's worth of data. 
            data_chunk = all_data_fp.read(struct_fmt.size * 2000) # data_chunk here is now more like a buffer

            # sys.stderr.write("{0}\n".format(len(data_chunk)) )

            # In python, once EoF is reached, we'll just read an emptry string (even for binary files), so check if chunk size is 0. This didn't work for iter_unpack for some reason, so I replaced the test for empty string with the test for len(data_chunk) == 0.            
            if len(data_chunk) == 0:
                break

            gen = struct_fmt.iter_unpack(data_chunk)

            for elem in gen:

                ipid, sent, succ, au, err, loss = elem
            
                ipstr = socket.inet_ntoa(struct.pack('!L', ipid))

                sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(ipstr, sent, succ, au, err, loss) )

            data_chunk = ''
    
    # sys.exit(1)
