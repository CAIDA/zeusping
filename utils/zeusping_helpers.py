
import shlex
import subprocess
import sys
import radix
import re
import os
import struct
import socket

ROUND_SECS = 600

class FileEmptyError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value


def load_radix_tree(pfx2AS_fn, rtree, py_ver=2):
    # try:
    #     if os.stat(pfx2AS_fn).st_size == 0:
    #         raise FileEmptyError('file is empty')
    #     sys.stderr.write('reading pfx2AS file {0}\n'.format(pfx2AS_fn) )

    #     pfx2AS_fp = open(pfx2AS_fn,'r')
    # except OSError as o:
    #     sys.stderr.write('pfx2AS file error: {0}\n'.format(o))
    #     sys.exit(1)
    # except FileEmptyError as f:
    #     sys.stderr.write('pfx2AS file error: {0}\n'.format(f))
    #     no_pfx2asfile=1
    #     sys.exit(1)
    # except IOError as i:
    #     sys.stderr.write('File open failed: {0}\n'.format(i) )
    #     sys.exit(1)
    
    if os.stat(pfx2AS_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading pfx2AS file {0}\n'.format(pfx2AS_fn) )

    try:
        zcat_cmd = "zcat {0}".format(pfx2AS_fn)
        sys.stderr.write("{0}\n".format(zcat_cmd) )
        proc = subprocess.Popen(zcat_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("zcat failed for {0}; exiting\n".format(zcat_cmd) )
        sys.exit(1)

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            if py_ver == 3:
                line = line.decode()
            if re.match(r'#', line): continue
            fields = line.strip().split()
            if len(fields) != 3: continue
            rnode = rtree.add(fields[0]+'/'+fields[1])
            rnode.data["origin"] = fields[2]

    proc.wait() # Wait for the subprocess to exit            
        

def ipint_to_ipstr(ipint):
    return socket.inet_ntoa(struct.pack("!I", ipint))


def ipstr_to_ipint(ipstr):
    try:
        ipint = struct.unpack("!I", socket.inet_aton(ipstr))[0]
    except socket.error:
        ipint = -1
    return ipint
    

def load_idx_to_dicts(loc_fname, idx_to_loc_fqdn, idx_to_loc_name, idx_to_loc_code, ctry_code_to_fqdn=None, ctry_code_to_name=None, py_ver=3):
    read_cmd = 'zcat {0}'.format(loc_fname)
    args = shlex.split(read_cmd)

    try:
        proc = subprocess.Popen(read_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("read cmd failed for {0}; exiting\n".format(read_cmd) )

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            if py_ver == 3:
                parts = line.decode().strip().split(',')
            elif py_ver == 2:
                parts = line.strip().split(',')
            idx = parts[0].strip()
            fqdn = parts[1].strip()
            idx_to_loc_fqdn[idx] = fqdn
            loc_name = parts[2][1:-1] # Get rid of quotes
            idx_to_loc_name[idx] = loc_name
            loc_code = parts[3]
            idx_to_loc_code[idx] = loc_code

            if ctry_code_to_fqdn is not None:
                ctry_code_to_fqdn[loc_code] = fqdn

            if ctry_code_to_name is not None:
                ctry_code_to_name[loc_code] = loc_name


def build_setofints_from_file(fname):
    s = set()
    fp = open(fname, 'r')
    for line in fp:
        idx = int(line[:-1].strip() )
        s.add(idx)
    fp.close()
    return s


def build_setofstrs_from_file(fname):
    s = set()
    fp = open(fname, 'r')
    for line in fp:
        idx = line[:-1].strip()
        s.add(idx)
    fp.close()        
    return s


# TODO: This function will change once we implement a better per-s24 file representation
def find_addrs_in_s24_with_status(s24, val, status, s24_to_dets):

    s24_pref = s24[:-4]
    
    curr_oct4 = 0
    for bit_pos in range(256):

        if( ( (val >> bit_pos) & 1) == 1):
            addr = "{0}{1}".format(s24_pref, curr_oct4)
            s24_to_dets[status].add(addr)

        curr_oct4 += 1


