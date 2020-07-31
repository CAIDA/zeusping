
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
