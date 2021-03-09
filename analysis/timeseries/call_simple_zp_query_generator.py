
import sys
import os
import subprocess

fqdn = sys.argv[1]
alias = sys.argv[2]
# asn_list = ['8048', '8053', '27889', '21826', '11562'] # VE
asn_list = ['8048', '21826', '11562'] # VE Tachira

py_cmd = "python simple_zp_query_generator.py {0} {1}".format(fqdn, alias)
print subprocess.check_output(py_cmd, shell=True)

for asn in asn_list:
    py_cmd = "python simple_zp_query_generator.py {0}.asn.{1} {2}_AS{1}".format(fqdn, asn, alias)
    # sys.stderr.write("{0}\n".format(py_cmd) )
    # os.system(py_cmd)
    print subprocess.check_output(py_cmd, shell=True)
