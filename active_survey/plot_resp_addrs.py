
import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import math
import getopt
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

plt.rcParams.update({'figure.autolayout': True}) # This ensures that labels won't be snipped off.

def labelrect(rects, y_arr):
    ct = 0
    for rect in rects:
        this_y = y_arr[ct]
        height = rect.get_height()
        ax.annotate('{0:.0f}'.format(this_y*100),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

        ct += 1

        
def populate_asn_to_resp(inp_fname, xlabel):
    

    fp = open(inp_fname)
    aggr_to_resp = {}
    for line in fp:

        parts = line.strip().split('|')

        if len(parts) != 6:
            continue

        region = parts[0]
        asn = parts[1]
        tot_resp = int(parts[2])
        tot = int(parts[4])
        pct_resp = float(parts[5])

        # If the xlabel is "AS", we're plotting % resp for various ASes in a location
        # If the xlabel is "Country" or "State", we're plotting % resp for various regions for a given AS
        if xlabel == "AS":
            aggr_to_resp[asn] = {"region" : region, "asn" : asn, "tot" : tot, "tot_resp" : tot_resp, "pct_resp" : float(tot_resp)/tot}
        elif ( (xlabel == "Country") or (xlabel == "State") ):
            aggr_to_resp[region] = {"region" : region, "asn" : asn, "tot" : tot, "tot_resp" : tot_resp, "pct_resp" : float(tot_resp)/tot}

    return aggr_to_resp

        
if __name__=="__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:r:a:o:x:y:m:l:t:u", ["inpfile=", "inpasn=", "inpregion=", "opfile=", "xlabel=", "ylabel=", "mode=", "islegend=", "istalk=", "usage="])
        
    except getopt.GetoptError as err:
        print (str(err))
        usage(sys.argv)
        sys.exit(1)

    inpfile = None
    xlabel = "AS"
    ylabel = "Addresses"
    mode = 'single' # By default, we will assume that we are plotting a single region/AS, specified in inpfile
    istalk = 0
    islegend = 1
    op_fname = None

    for o, a in opts:
        if o in ("-i", "--inpfile"):
            inpfile = a
        elif o in ("-r", "--inpregion"):
            inpregion = a
        elif o in ("-a", "--inpasn"):
            inpasn = a
        elif o in ("-o", "--opfile"):
            op_fname = a
        elif o in ("-x", "--xlabel"):
            xlabel = a
        elif o in ("-y", "--ylabel"):
            ylabel = a
        elif o in ("-m", "--mode"):
            mode = a
        elif o in ("-l", "--islegend"):
            islegend = int(a)
        elif o in ("-t", "--istalk"):
            istalk = int(a)
        elif o in ("-u", "--usage"):
            usage(sys.argv)
            sys.exit(1)
        else:
            assert False, "unhandled option"


    if op_fname is None:
        op_fname = "{0}_resp_addrs.eps".format(inp_fname)
            
    aggr_to_resp = {} # For a given aggregate (Country/U.S. state/AS), this dict will keep track of how many addresses were pinged and how many were responsive.

    if mode == "single":
        inp_fname = inpfile

        aggr_to_resp = populate_asn_to_resp(inp_fname, xlabel)    

        sorted_d = sorted(aggr_to_resp.items(), key=lambda kv: kv[1]["tot"], reverse=True)
        # print(sorted_d)

        # TODO: This is a list of ASes for CA. Think about how we can generalize this list of ASes for arbitrary states and/or countries
        # reqd_asns = ['7018','20001','7922','5650','22773','7132','701','20115','4565','2828','16591','18566','3561','22394','7065','7029','46375','209','7155']

        fig, ax = plt.subplots(figsize=(5, 2.625))
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)

        x_arr = []
        y_arr = {"tot_resp" : [], "tot" : [], "pct_resp" : []}
        x_val = 0
        max_vals = 15
        for tup in sorted_d:
            aggr = tup[0]
            val = tup[1]

            # if asn not in reqd_asns:
            #     continue

            x_arr.append(aggr)
            y_arr["tot_resp"].append(val["tot_resp"])
            y_arr["tot"].append(val["tot"])
            y_arr["pct_resp"].append(val["pct_resp"])
            x_val += 1

            if x_val == max_vals:
                break

        x_pos = np.arange(x_val)

        print(x_pos)
        print(y_arr)
        
        width=0.8
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_arr)
        rects = ax.bar(x_pos, y_arr["tot"], width, align='center', color=def_colors[0], label="Pinged")        
        ax.bar(x_pos, y_arr["tot_resp"], width, align='center', color=def_colors[1], label="Responsive")
        labelrect(rects, y_arr["pct_resp"])
        plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
        # ax.grid()
        ax.yaxis.set_minor_locator(AutoMinorLocator()) 
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    fig.tight_layout()
    fig.savefig(op_fname)
    # plt.show()



