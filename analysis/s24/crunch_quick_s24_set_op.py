
#  This software is Copyright (c) 2020 The Regents of the University of
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
import os

idir = sys.argv[1] # input dir
asn = sys.argv[2]

print "Total /24s probed:"
os.system("wc -l {0}/{1}_s24".format(idir, asn) )

print "\nTotal /24s that experienced a dropout this round:"
os.system('''awk '{{print $2}}' {0}/{1}_s24_set | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 5 addresses with a dropout:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) ) print}}' | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 10 addresses with a dropout:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) ) print}}' | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 5 addresses with a dropout and at least one responsive address:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) && ($3 > 0) ) print}}' | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 10 addresses with a dropout and at least one responsive address:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) && ($3 > 0) ) print}}' | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 5 addresses with a dropout and no responsive addresses:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) && ($3 == 0) ) print}}' | wc -l'''.format(idir, asn) )

print "\n/24s that had at least 10 addresses with a dropout and no responsive addresses:"
os.system('''awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) && ($3 == 0) ) print}}' | wc -l'''.format(idir, asn) )


print "\nUpper limit on addresses involved in the correlated dropout (includes noise):"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 5 dropouts:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 10 dropouts:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 5 dropouts and with at least one responsive address:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) && ($3 > 0) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 10 dropouts and with at least one responsive address:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) && ($3 > 0) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 5 dropouts and no responsive addresses:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 5) && ($3 == 0) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )

print "\nAddresses involved in the correlated dropout from /24s with at least 10 dropouts and no responsive addresses:"
os.system("awk '{{print $1, $2}}' {0}/{1}_s24_set | awk -F '|' '{{if ( ($2 >= 10) && ($3 == 0) ) print $2}}' |  awk '{{SUM+=$1}} END {{print SUM}}'".format(idir, asn) )
