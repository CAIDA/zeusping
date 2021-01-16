
import sys

campaign = sys.argv[1]
num_splits = 2

# vps = [
#     {"loc" : "atlanta1", "plat" : "lin"},
#     {"loc" : "sf1", "plat" : "doc"},
#     {"loc" : "tor1", "plat" : "doc"},
#     {"loc" : "nyc1", "plat" : "doc"},
#     ]

# vps = [
#     {"loc" : "fremont2", "plat" : "lin"},
#     {"loc" : "dallas2", "plat" : "doc"},
#     {"loc" : "newark2", "plat" : "doc"},
#     ]

vps = [
    {"loc" : "fremont3", "plat" : "lin"},
    {"loc" : "dallas3", "plat" : "lin"},
    {"loc" : "newark3", "plat" : "lin"},
    ]

for vp in vps:
    for i in range(num_splits):
        # cmd = "sudo /home/ramapad/scamper_2019/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/ramapad/zeusping/for_testing/op_{1}/{2}{0}{3} -I 1800 -R 600 -c 'ping -c 1'".format(i+1, campaign, vp["plat"], vp["loc"])
        # For testing Scamper's -I with larger values
        cmd = "sudo /home/ramapad/scamper_2019/bin/sc_erosprober -U scamper_{0}.sock -a {1}sp{0} -o /home/ramapad/zeusping/for_testing/op_{1}/survey{2}{0}{3} -I 1800 -R 600 -c 'ping -c 1'".format(i+1, campaign, vp["plat"], vp["loc"])
        sys.stderr.write("{0}\n".format(cmd) )
