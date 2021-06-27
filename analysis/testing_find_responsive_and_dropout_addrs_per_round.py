
import sys
import os
import subprocess
import shlex

if sys.version_info[0] == 2:
    py_ver = 2
    import wandio
    import subprocess32
    from sc_warts import WartsReader
else:
    py_ver = 3

for round_tstamp in range(1617495000, 1617753600, 600):
    
    # First sort the new format file
    new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/{0}_to_{1}/rda.gz".format(round_tstamp, round_tstamp + 600)
    temp_new_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/{0}_to_{1}/temp_rda".format(round_tstamp, round_tstamp + 600)
    cmd = "zcat {0} | sort > {1}".format(new_format_fname, temp_new_format_fname)
    print(cmd)
    # args = shlex.split(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # with proc.stderr:
    #     for line in iter(proc.stderr.readline, b''):
    #         print(line)

    # Next sort the old format file
    old_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/{0}_to_{1}.gz".format(round_tstamp, round_tstamp + 600)
    temp_old_format_fname = "/scratch/zeusping/data/processed_op_CA_ME_testsimple/responsive_and_dropout_addrs_1617495000to1617753600/temp_{0}_to_{1}".format(round_tstamp, round_tstamp + 600)
    cmd = "zcat {0} | sort > {1}".format(old_format_fname, temp_old_format_fname)
    print(cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # Run diff
    diff_cmd = "diff {0} {1} | wc -l".format(temp_old_format_fname, temp_new_format_fname)
    print(diff_cmd)
    # proc = subprocess.Popen(diff_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    # proc.wait()
    # print("Return code: {0}".format(proc.returncode) )
    # with proc.stdout:
    #     for line in iter(proc.stdout.readline, b''):
    #         print(line)
    proc = subprocess.run(diff_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True) # Note: No need to explicitly called wait if we call run()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)
    if proc.stdout != b'0\n':
        sys.stderr.write("Outputs did not match!\n")
        sys.exit(1)
    
    
    # delete old format file
    rm_old_cmd = "rm {0}".format(temp_old_format_fname)
    print(rm_old_cmd)
    proc = subprocess.Popen(rm_old_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)

    # Delete new format file
    rm_new_cmd = "rm {0}".format(temp_new_format_fname)
    print(rm_new_cmd)
    proc = subprocess.Popen(rm_new_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc.wait()
    print("Return code: {0}".format(proc.returncode) )
    if proc.returncode != 0:
        sys.exit(1)
    
    # sys.exit(1)
    
