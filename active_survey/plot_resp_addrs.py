
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

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import math
import getopt
import matplotlib.ticker as ticker
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def_colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

plt.rcParams.update({'figure.autolayout': True}) # This ensures that labels won't be snipped off.

def labelrect_bar(ax, rects, y_arr):
    ct = 0
    for rect in rects:
        this_y = y_arr[ct]
        height = rect.get_height()
        # print(height)
        # print(rect.get_x() )
        # print(rect.get_width())
        ax.annotate('{0:.0f}%'.format(this_y*100),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

        ct += 1

        
def labelrect_barh(ax, rects, y_arr):
    ct = 0
    for rect in rects:
        this_y = y_arr[ct]
        width = rect.get_width()
        # print(width)
        # print(rect.get_y() )
        # print(rect.get_height())
        ax.annotate('{0:.0f}%'.format(this_y*100),
                    xy=(width, rect.get_y() + rect.get_height() / 2),
                    xytext=(3, 0),  # 3 points horizontal offset
                    textcoords="offset points",
                    ha='left', va='center')

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

        if asn == 'UNK':
            continue
        elif '_' in asn:
            continue
        
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
        opts, args = getopt.getopt(sys.argv[1:], "i:r:a:o:x:y:m:h:l:t:u", ["inpfile=", "inpasn=", "inpregion=", "opfile=", "xlabel=", "ylabel=", "mode=", "isbarh=", "islegend=", "istalk=", "usage="])
        
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
    isbarh = 1

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
        elif o in ("-h", "--isbarh"):
            isbarh = int(a)
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
        print(sorted_d)

        # TODO: This is a list of ASes for CA. Think about how we can generalize this list of ASes for arbitrary states and/or countries
        # reqd_asns = ['7018','20001','7922','5650','22773','7132','701','20115','4565','2828','16591','18566','3561','22394','7065','7029','46375','209','7155']

        if isbarh == 0:
            fig, ax = plt.subplots(figsize=(8, 3.625))
            # ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
        else:
            fig, ax = plt.subplots(figsize=(2.625, 4))
            ax.set_ylabel(xlabel)
            ax.set_xlabel(ylabel)

        x_arr = []
        y_arr = {"tot_resp" : [], "tot" : [], "pct_resp" : []}
        x_val = 0
        max_vals = 20
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

        if isbarh == 0:
            ax.set_xticks(x_pos)
            ax.set_xticklabels(x_arr)
            rects = ax.bar(x_pos, y_arr["tot"], width, align='center', color=def_colors[0], label="Pinged")        
            ax.bar(x_pos, y_arr["tot_resp"], width, align='center', color=def_colors[1], label="Responsive")
            labelrect_bar(ax, rects, y_arr["pct_resp"])
            plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
            # ax.grid()
            ax.yaxis.set_major_formatter(ticker.EngFormatter())
            ax.yaxis.set_minor_locator(AutoMinorLocator()) 
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)
        else:
            ax.set_yticks(x_pos)
            ax.set_yticklabels(x_arr)
            rects = ax.barh(x_pos, y_arr["tot"], width, align='center', color=def_colors[0], label="Pinged")        
            ax.barh(x_pos, y_arr["tot_resp"], width, align='center', color=def_colors[1], label="Responsive")
            labelrect_barh(ax, rects, y_arr["pct_resp"])
            plt.setp(ax.get_yticklabels(), rotation=0, ha='right')
            # ax.grid()
            ax.xaxis.set_major_formatter(ticker.EngFormatter())
            ax.xaxis.set_minor_locator(AutoMinorLocator()) 
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)


        lgd = ax.legend(bbox_to_anchor=(0,1.02,1,0.2), loc="upper center", ncol=2)
        
    elif mode == "1r3c":

        # 1 row and 3 columns for barh
        
        inp_fnames = []
        fp = open(inpfile)
        for line in fp:
            inp_fnames.append(line[:-1])
                
        if isbarh == 0:
            fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(10, 2.625))
            # ax.set_xlabel(xlabel)
            # ax.set_ylabel(ylabel)
        else:
            fig, ax = plt.subplots(nrows=1, ncols=3, figsize=(10, 4))

        # i = 0
        j = 0
        for inp_fname in inp_fnames:

            inp_fname_parts = inp_fname.strip().split('_')
            ctry_code = inp_fname_parts[0][-2:]

            new_ylabel = "{0} {1}".format(ctry_code, ylabel)
            
            ax[j].set_ylabel(xlabel)
            ax[j].set_xlabel(new_ylabel)

            aggr_to_resp = populate_asn_to_resp(inp_fname, xlabel)

            sorted_d = sorted(aggr_to_resp.items(), key=lambda kv: kv[1]["tot"], reverse=True)

            x_arr = []
            y_arr = {"tot_resp" : [], "tot" : [], "pct_resp" : []}
            x_val = 0
            max_vals = 10
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

            width=0.8
            
            ax[j].set_yticks(x_pos)
            ax[j].set_yticklabels(x_arr)

            if j == 1:
                rects = ax[j].barh(x_pos, y_arr["tot"], width, align='center', color=def_colors[0], label="Pinged")
                ax[j].barh(x_pos, y_arr["tot_resp"], width, align='center', color=def_colors[1], label="Responsive")
            else:
                rects = ax[j].barh(x_pos, y_arr["tot"], width, align='center', color=def_colors[0])
                ax[j].barh(x_pos, y_arr["tot_resp"], width, align='center', color=def_colors[1])
                
            labelrect_barh(ax[j], rects, y_arr["pct_resp"])
            plt.setp(ax[j].get_yticklabels(), rotation=0, ha='right')
            ax[j].xaxis.set_major_formatter(ticker.EngFormatter())
            ax[j].xaxis.set_minor_locator(AutoMinorLocator())
            ax[j].spines['right'].set_visible(False)
            ax[j].spines['top'].set_visible(False)

        
            if j == 1:
                lgd = ax[j].legend(bbox_to_anchor=(0,1.02,1,0.2), loc="upper center", ncol=2)
            
            j += 1

        # lgd = fig.legend(bbox_to_anchor=(0,1.02,1,0.2), loc="lower left", ncol=2)            
        # fig.legend(ncol=2)
        
    # fig.tight_layout()
    # fig.savefig(op_fname)
    fig.savefig(op_fname, bbox_extra_artists=(lgd,), bbox_inches="tight")
    # plt.show()



