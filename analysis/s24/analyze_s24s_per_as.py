
import sys
import glob
import shlex
import subprocess
import os
import datetime
import json
from collections import defaultdict
import array
import io
import struct
import socket

zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import zeusping_helpers

# We'd like to find how many addresses we pinged in each /24
# How many of those addresses responded to pings in general
# How many addresses responded to pings from the /24s that Marder cared about

def split_ips(ip):
    parts = ip.strip().split('.')
    # print parts
    oct1 = parts[0].strip()
    oct2 = parts[1].strip()
    oct3 = parts[2].strip()
    oct4 = parts[3].strip()

    return oct1, oct2, oct3, oct4


def find_s24(ipv4_addr):
    oct1, oct2, oct3, oct4 = split_ips(ipv4_addr)
    return "{0}.{1}.{2}.0/24".format(oct1, oct2, oct3)


def populate_s24_to_dets(fname, typ):
    fp = open(fname)

    if typ == "pinged":
        pinged_addrs = set()

    for line in fp:

        parts = line.strip().split('|')
        addr = parts[0].strip()

        if typ == "pinged":
            pinged_addrs.add(addr)

        s24 = find_s24(addr)

        if s24 not in s24_to_dets:
            # s24_to_dets[s24] = {"pinged" : set(), "resp" : set()}
            s24_to_dets[s24] = {"pinged" : set(), "resp" : set(), "d" : set(), "r" : set(), "a" : set()}

        s24_to_dets[s24][typ].add(addr)

    if typ == "pinged":
        return pinged_addrs

    
def populate_s24_to_round_status(fname, reqd_set):
    fp = open(fname)

    for line in fp:

        parts = line.strip().split()
        
        addr = parts[0].strip()

        if addr not in reqd_set:
            continue
        
        status = parts[1].strip()

        s24 = find_s24(addr)

        if s24 not in s24_to_dets:
            # s24_to_dets[s24] = {"pinged" : set(), "resp" : set()}
            s24_to_dets[s24] = {"pinged" : set(), "resp" : set(), "spec" : set()}

        s24_to_dets[s24][status_to_char[status]].add(addr)

        
pinged_ips_fname = sys.argv[1]
resp_ips_fname = sys.argv[2]
specific_round_fname = sys.argv[3]

status_to_char = {"0" : "d", "1" : "r", "2" : "a"}
s24_to_dets = {}

pinged_addrs = populate_s24_to_dets(pinged_ips_fname, "pinged")
populate_s24_to_dets(resp_ips_fname, "resp")
populate_s24_to_round_status(specific_round_fname, pinged_addrs)

# for s24 in s24_to_dets:
#     sys.stdout.write("{0} {1} {2}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["resp"]) ) )

for s24 in s24_to_dets:
    # sys.stdout.write("{0} {1} {2} {3} {4} {5}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["resp"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]),  len(s24_to_dets[s24]["a"]) ) )
    sys.stdout.write("{0} {1} {2} {3}\n".format(s24, len(s24_to_dets[s24]["pinged"]), len(s24_to_dets[s24]["d"]),  len(s24_to_dets[s24]["r"]) ) )
    
