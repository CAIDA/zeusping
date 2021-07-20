#!/usr/bin/env python

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

# I was frustrated that no matter what buffer setting I passed to communicate,
# I could not get stdout from my subprocess until the process had completed.
# I googled around and came up with this, which illustrates the problem and a
# solution.

# http://stackoverflow.com/questions/2804543/read-subprocess-stdout-line-by-line
# http://bugs.python.org/issue3907
# http://docs.python.org/library/io.html

import io
import os.path
import subprocess
import sys
import time

def generate():
    for i in xrange(0, 20):
        print 'line ' + str(i)
        sys.stdout.flush()
        time.sleep(0.3)

def invoke_subprocess(bufsize):
    return subprocess.Popen('python ' + os.path.abspath(__file__) + ' --generate', shell=True, stdout=subprocess.PIPE, bufsize=bufsize)

def subprocess_communicate(bufsize):
    p = invoke_subprocess(bufsize)
    while p.returncode is None:
        (stdout, stderr) = p.communicate()
        for line in stdout.splitlines():
            yield line

def io_open():
    p = invoke_subprocess(1)
    for line in io.open(p.stdout.fileno()):
        yield line.rstrip('\n')

def demo():
    for (name, fn) in {
        'unbuffered p.communicate()': lambda: subprocess_communicate(0),
        'line buffered p.communicate()': lambda: subprocess_communicate(1),
        'io.open(p.stdout)': lambda: io_open(),
    }.iteritems():
        start = time.time()
        first = None
        stop = None
        lines = []
        for line in fn():
            lines.append(line)
            if first is None:
                first = time.time()
        stop = time.time()
        print name + ': ' + str(len(lines)) + ' received'
        if first is not None:
            print name + ': ' + str(first - start) + ' seconds until first line'
        print name + ': ' + str(stop - start) + ' seconds until all lines'

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '--generate':
        generate()
    else:
        demo()
