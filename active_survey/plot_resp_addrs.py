
import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import math

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

plt.rcParams.update({'figure.autolayout': True}) # This ensures that labels won't be snipped off.

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


def populate_asn_to_resp(inp_fname):
    

    fp = open(inp_fname)
    asn_to_resp = {}
    for line in fp:

        parts = line.strip().split()

        if len(parts) != 6:
            continue

        asn = parts[1]
        tot_resp = int(parts[2])
        tot = int(parts[4])
        pct_resp = float(parts[5])

        asn_to_resp[asn] = {"tot" : tot, "tot_resp" : tot_resp, "pct_resp" : float(tot_resp)/tot}

    return asn_to_resp

        

xlabel = "AS"
ylabel = "% responsive"

inp_fname = sys.argv[1]

asn_to_resp = populate_asn_to_resp(inp_fname)    

sorted_d = sorted(asn_to_resp.items(), key=lambda kv: kv[1]["pct_resp"], reverse=True)
# print(sorted_d)

# TODO: This is a list of ASes for CA. Think about how we can generalize this list of ASes for arbitrary states and/or countries
reqd_asns = ['7018','20001','7922','5650','22773','7132','701','20115','4565','2828','16591','18566','3561','22394','7065','7029','46375','209','7155']

fig, ax = plt.subplots(figsize=(5, 2.625))
ax.set_xlabel(xlabel)
ax.set_ylabel(ylabel)

x_arr = []
y_arr = []
x_val = 0
for tup in sorted_d:
    asn = tup[0]
    val = tup[1]

    if asn not in reqd_asns:
        continue

    x_arr.append(asn)
    y_arr.append(val["pct_resp"])
    x_val += 1

x_pos = np.arange(len(y_arr))

print(x_pos)
print(y_arr)
width=0.9
ax.set_xticks(x_pos)
ax.set_xticklabels(x_arr)
ax.bar(x_pos, y_arr, width, align='center')
plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
ax.grid()
fig.savefig("{0}_resp_addrs.eps".format(inp_fname))
# plt.show()

    

