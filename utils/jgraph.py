
# This class can be inherited and used in a program which then generates a jgr file with its own custom settings.
# NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful

# Usage e.g: python jgraph.py -i inpfile -o all_conn_durs_cdf_new.jgr -x "Connection Duration (s)" -y CDF

# TODO: Replace os.system with subprocess.

import sys
import getopt
import time

# For executing commands
import os
import shlex, subprocess
import time

import numpy as np
import scipy.stats as stats
import random
import math


def find_ci_prop_diff(sample_successes_p1, sample_size_p1, sample_successes_p2, sample_size_p2, conf_level):

    sample_prop_p1 = sample_successes_p1/float(sample_size_p1)
    sample_prop_p2 = sample_successes_p2/float(sample_size_p2)

    sample_prop_diff = sample_prop_p1 - sample_prop_p2

    se_squared = (sample_prop_p1 * (1 - sample_prop_p1) )/float(sample_size_p1) + (sample_prop_p2 * (1 - sample_prop_p2) )/float(sample_size_p2)
    se = math.sqrt(se_squared)

    alpha = 1 - conf_level/100.0
    critical_probability = 1 - alpha/2.0
    z_critical = stats.norm.ppf(critical_probability)

    margin_of_error = z_critical * se

    low = sample_prop_diff - margin_of_error
    high = sample_prop_diff + margin_of_error

    return low, sample_prop_diff, high


def find_ci_prop(sample_successes, sample_size, conf_level):

    sample_prop = sample_successes/float(sample_size)

    se_squared = (sample_prop * (1 - sample_prop) )/float(sample_size)
    se = math.sqrt(se_squared)

    # At x = 0.5, z = 0
    # 100% of values above the mean lie beyond x = 0.5
    # To get 95th percent value between 0.5 to 1: 0.5 + (1 - 0.5)*0.95
    # q = 0.5 + (1 - 0.5) * conf_level/100.0

    alpha = 1 - conf_level/100.0
    critical_probability = 1 - alpha/2.0
    z_critical = stats.norm.ppf(critical_probability)

    margin_of_error = z_critical * se

    low = sample_prop - margin_of_error
    high = sample_prop + margin_of_error

    return low, sample_prop, high


def is_gaussian_approximation_ok(sample_size, sample_prop):
    # First, let's see if this passes test 1
    if ( (sample_size*sample_prop > 10) and (sample_size * (1-sample_prop) > 10) ):
        # Check if this passes test 2
        thresh = sample_size * sample_prop * (1 - sample_prop)
        if (thresh > 10):
            return True
        else:
            return False
        
    else:
        return False

    
def exec_cmd(cmd_with_args, op_fname = None):
    # sys.stderr.write("{0}\n".format(cmd))
    try:
        if (op_fname != None):
            with open(op_fname, "w") as op_fp:
                # args = shlex.split(cmd_with_args)
                p = subprocess.Popen(cmd_with_args, bufsize=0, stdout = subprocess.PIPE, shell=True)
                while True:
                    line = p.stdout.readline()
                    if not line:
                        break
                    print line

                # op_fp.flush()
                retcode = p.wait()
                # op_fp.flush()
                # os.waitpid(p.pid, 0)
                # time.sleep(5)
                # retcode = p.communicate()
        else:
            # retcode = subprocess.call(cmd_with_args, shell=True)
            p = subprocess.Popen(cmd_with_args, bufsize=0, shell=True)
            retcode = p.communicate()
        if retcode < 0:
            sys.stderr.write("Child was terminated by signal: {0}".format(retcode))
        else:
            sys.stderr.write("Child returned: {0}\n".format(retcode))
    except OSError as e:
        sys.stderr.write("Execution failed: {0}".format(e))


class Jgraph:
    # Have all the attributes that I want to be explicit definied here
    def __init__(self):
        self.op_fname = None
        self.fp = None
        self.xmin = None
        self.xmax = None
        self.remove_xoffset = None
        self.xlog = False
        self.ylog = False

    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))
        self.xlog = False
        self.ylog = False

    def set_xlog(self):
        self.xlog = True

    def set_ylog(self):
        self.ylog = True

    def newgraph(self, s):
        self.fp.write("newgraph {0}\n".format(s))

    def config_xaxis(self, xaxis_label):
        conf = '\n'
        if (self.xlog):
            conf += 'log\n'
            conf += 'hash_format g\n'
        conf += 'size 4\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(xaxis_label)
        return conf

    def xaxis(self, xlabel):
        self.fp.write("xaxis {0}\n".format(self.config_xaxis(xlabel)))

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        return conf

    def yaxis(self, ylabel):
        self.fp.write("yaxis {0}\n".format(self.config_yaxis(ylabel)))

    def title(self, s):
        self.fp.write("title : {0}\n".format(s))

    def plot_histo(self, cts):
        for ct in cts:
            self.fp.write("newcurve marktype xbar marksize 0.1 pts {0} {1}\n".format(ct, cts[ct]))

    def fix_ipecho_in_fname(self, f):
        new_dst_fname = ''

        if (f.find('ipecho') != -1):
            ipecho_idx = f.find('ipecho')
            underscore_idx = ipecho_idx + len('ipecho')
            new_dst_fname = f[:underscore_idx] + "-" + f[(underscore_idx+1):]

        return new_dst_fname

    def copy_cdf_files(self, pathh, inpfiles_fp):
        dst_fnames = []
        for f in inpfiles_fp:
            src_fname = f[:-1]
            # dst_fname = self.fix_ipecho_in_fname(src_fname) # No need to 'fix' ipecho in fname because I'm just using ipechov4 as the suffix instead of ipecho_v4
            dst_fname = src_fname
            if (dst_fname == ''):
                dst_fnames.append(src_fname)
            else:
                dst_fnames.append(dst_fname)
            src_path = '{0}/{1}'.format(pathh, src_fname)
            dst_path = './' + dst_fname
            cp_cmd = 'cp {0} {1}'.format(src_path, dst_path)
            sys.stderr.write('{0}\n'.format(cp_cmd))
            os.system(cp_cmd)

        return dst_fnames

    def exec_jgraph(self):
        # Run jgraph cmd
        eps_fname = self.op_fname[:-3] + "eps"
        jgr_cmd = "env JGRAPH_BORDER=6 jgraph " + self.op_fname + " > " + eps_fname
        sys.stderr.write("{0}\n".format(jgr_cmd))
        # os.system(jgr_cmd)

        # time.sleep(2)

        # jgr_cmd_args = []
        # jgr_cmd_args.append('env')
        # jgr_cmd_args.append('JGRAPH_BORDER=6')
        # jgr_cmd_args.append('jgraph')
        # jgr_cmd_args.append(self.op_fname)

        # Latest popen attempts:
        # jgr_cmd_args = "env JGRAPH_BORDER=6 jgraph " + self.op_fname
        # sys.stderr.write("{0}\n".format(jgr_cmd_args))
        # exec_cmd(jgr_cmd_args, op_fname = eps_fname)

        # time.sleep(2)

        # Run epstopdf

        eps2pdf_cmd = "epstopdf " + eps_fname

        # Latest popen attempts:
        # exec_cmd(eps2pdf_cmd)

        sys.stderr.write("{0}\n".format(eps2pdf_cmd))
        # os.system(eps2pdf_cmd)

        # epstopdf sometimes takes a while to complete. So sleep for a while.
        # time.sleep(2)

    def scp_to_bp(self, bp_path):
        pdf_fname = self.op_fname[:-3] + "pdf"
        scp_cmd = "scp " + pdf_fname + " " + bp_path
        sys.stderr.write("{0}\n".format(scp_cmd))
        # os.system(scp_cmd)
        # exec_cmd(scp_cmd)

    def cleanup_fnames(self, dst_fnames):
        for dst_fname in dst_fnames:
            rm_cmd = 'rm {0}'.format(dst_fname)
            sys.stderr.write('{0}\n'.format(rm_cmd))
            os.system(rm_cmd)


class Bar_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.barcolors = [
        "1 0.5 0",
        "0.5 0.5 1"
        ]
        self.barcomponents = []
        self.bar_y_vals = {}
        self.bar_x_vals = []
        self.xlog = False
        self.ylog = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))

    def config_xaxis(self, xaxis_label):
        conf = '\n'
        conf += 'size 4\n'
        conf += 'no_auto_hash_marks\n'
        conf += 'no_auto_hash_labels\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(xaxis_label)
        conf += "min 0\n"
        conf += "max {0}\n".format(max(self.bar_x_vals))

        return conf

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        conf += 'min 0\n'
        return conf


    def set_bar_x_vals(self, x_vals):
        self.bar_x_vals = x_vals

    def set_bar_y_vals(self, y_vals):
        self.bar_y_vals = y_vals

    def set_bar_xlabels(self):
        start_idx = 0.5
        for x in self.bar_x_vals:
            # print x
            self.fp.write("hash_label at {0} : {1}\n".format(start_idx, x))
            start_idx += 1

    def set_bar_components(self, components):
        for comp in components:
            self.barcomponents.append(comp)

    def find_max_y(self):
        max_y = 0 # TODO: This Won't work for negative numbers
        if (len(self.barcomponents) == 1):
            for x in range(len(self.bar_y_vals)): # TODO: Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50
                if (self.bar_y_vals[x] > max_y):
                    max_y = self.bar_y_vals[x]
        else:
            for x in range(len(self.bar_y_vals)): # TODO: Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50
                for y_val in self.bar_y_vals[x]:
                    if (y_val > max_y):
                        max_y = y_val

        return max_y

    def set_bar_legend(self, x_pos, y_pos, where = 'topright'):
        if (where == 'topright'):
            x_pos = len(self.bar_x_vals)
            y_pos = self.find_max_y()
            self.fp.write("""legend defaults x {0} y {1} hjr vjt fontsize 12\n\n""".format(x_pos, y_pos - (y_pos/95.0)))
        elif (where == 'topleft'):
            x_pos = 1
            y_pos = self.find_max_y()
            self.fp.write("""legend defaults x {0} y {1} hjl vjt fontsize 12\n\n""".format(x_pos, y_pos - (y_pos/95.0)))
        elif (where == 'outside'):
            self.fp.write("""legend defaults hjr vjb fontsize 12\n\n""".format(x_pos, y_pos - (y_pos/95.0)))
            pass

        color_idx = 0
        if (len(self.barcomponents) > 1):
            for comp in self.barcomponents:
                self.fp.write("newcurve marktype xbar color {0} label : {1}\n".format(self.barcolors[color_idx], comp))
                color_idx += 1
        else:
            self.fp.write("newcurve marktype xbar color 0 0 0 label : {0}\n".format(self.barcomponents[0]))


    def plot_bar(self):
        # In a bar graph, we will plot a value of y at various values of x, like in any other graph
        # However, the primary difference between a bar graph, and say, a CDF, is that the xaxis values have to be transformed to something else. Typically (always?), the xaxis values in a bar graph will increment by 1.
        # In a CDF, if there are no values for a particular xaxis value, then we don't need to plot a point for it, since there is a curve.
        # However, a bar graph requires something to be plotted at every value from 1 to max_x_axis_val. And hence we use a different variable, ct, for the xaxis values. And map xaxis values to the ct value

        ct = 0.5
        for x in range(len(self.bar_y_vals)): # Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50. However, works better if we are plotting a value per hour-of-the-day, for example, where we need to plot every value in the range(0, 24). This version forces us to recognize missing values, if any, since the code will throw an error.
        # for x in self.bar_x_vals: # Works better when x doesn't have a specific range. For example, if the xaxis is ASNs and yaxis contains a value per ASN, x is not going to range neatly from 0 to N.
            color_idx = 0
            if (len(self.barcomponents) == 1):
                # print "{0} {1} {2}\n".format(ct, x, self.bar_y_vals[x])
                self.fp.write("newcurve marktype xbar marksize 0.2 color 0 0 0 pts {0} {1}\n".format(ct, self.bar_y_vals[x]))
            else:
                for y_val in self.bar_y_vals[x]:
                    # print x
                    # print self.bar_y_vals[x]
                    self.fp.write("newcurve marktype xbar marksize 0.2 color {0} pts {1} {2}\n".format(self.barcolors[color_idx], ct, y_val))
                    color_idx += 1

            ct += 1


class Scatter_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))
        self.scatter_marktypes = ['x', 'diamond', 'box', 'circle', 'cross']
        self.scattercolors = [
        # "color 0.8 0.2 0.2",
        # "color 0.2 0.8 0.2",
        # "color 0.2 0.2 0.8",
        # "color 0.7 0.7 0.7",
        # "color 0.2 0.8 0.7",
        # "color 0.7 0.2 0.7",
        # "color 0.7 0.7 0.2",
        # "color 0 0 0",

        # For talk
        "color 0.1 0.65 0.65",
        "color 0.65 0.65 0.2",
        "color 0.65 0.3 0.65",
        "color 0.8 0 0",
        "color 0.2 0.8 0.1",
        "color 0.2 0.2 0.8",
        "color 0.3 0.3 0.3",

        # For paper?
        # "color 0.6 0.9 0.6",
        # "color 0.3 0.6 0.9",
        # "color 0.8 0.2 0.2",
        # "color 0.8 0.8 0.8",
        # "color 0 0 0",

        # Dunno for what...
        # "color 0.2 0.8 0.7",
        # "color 0.4 0.9 0.2",
        # "color 0.9 0 0.9",
        ]


    def get_scatter_fnames(self, scatter_fnames_list):
        self.scatter_fnames_list = scatter_fnames_list

    def plot_scatter(self, label=""):
        if (len(self.scatter_fnames_list) == 1):
            self.fp.write("newcurve marktype x linetype none color 0 0 0\n".format(self.scattercolors[-1]) )
            if (label != ""):
                self.fp.write("label : {0}\n".format(label) )
            self.fp.write("pts\n")
            scatter_fname = self.scatter_fnames_list[0]
            scatter_fp = open(scatter_fname, 'r')
            for line in scatter_fp:
                self.fp.write("{0}".format(line) )


class Whisker_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.whisker_marktypes = ['x', 'diamond', 'box', 'circle', 'cross']
        self.whiskercomponents = []
        self.whisker_y_vals = {}
        self.whisker_x_vals = []
        self.xlog = False
        self.ylog = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))

    def config_xaxis(self, xaxis_label):
        conf = '\n'
        conf += 'size 4\n'
        conf += 'no_auto_hash_marks\n'
        conf += 'no_auto_hash_labels\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(xaxis_label)
        conf += "min 0\n"
        conf += "max {0}\n".format(max(self.whisker_x_vals))

        return conf

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label  fontsize 13\n'
        # conf += 'hash_labels  fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        conf += 'min 0\n'
        return conf


    def set_whisker_x_vals(self, x_vals):
        self.whisker_x_vals = x_vals

    def set_whisker_y_vals(self, y_vals):
        self.whisker_y_vals = y_vals

    def set_whisker_xlabels(self):
        start_idx = 0.5
        for x in self.whisker_x_vals:
            # print x
            self.fp.write("hash_label at {0} : {1}\n".format(start_idx, x))
            start_idx += 1

    def set_whisker_components(self, components):
        for comp in components:
            self.whiskercomponents.append(comp)

    def find_max_y(self):
        max_y = 0 # TODO: This Won't work for negative numbers
        if (len(self.whiskercomponents) == 1):
            for x in range(len(self.whisker_y_vals)): # TODO: Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50
                if (self.whisker_y_vals[x] > max_y):
                    max_y = self.whisker_y_vals[x]
        else:
            for x in range(len(self.whisker_y_vals)): # TODO: Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50
                for y_val in self.whisker_y_vals[x]:
                    if (y_val > max_y):
                        max_y = y_val

        return max_y

    def set_whisker_legend(self, x_pos, y_pos, where = 'topright'):
        if (where == 'topright'):
            x_pos = len(self.whisker_x_vals)
            y_pos = self.find_max_y()
            self.fp.write("""legend defaults x {0} y {1} hjr vjt fontsize 12\n\n""".format(x_pos, y_pos - (y_pos/95.0)))
        elif (where == 'topleft'):
            x_pos = 1
            y_pos = self.find_max_y()
            self.fp.write("""legend defaults x {0} y {1} hjl vjt fontsize 12\n\n""".format(x_pos, y_pos - (y_pos/95.0)))

        mark_idx = 0
        if (len(self.whiskercomponents) > 1):
            for comp in self.whiskercomponents:
                self.fp.write("newcurve marktype {0} color 0 0 0 label : {1}\n".format(self.whisker_marktypes[mark_idx], comp))
                mark_idx += 1
        else:
            self.fp.write("newcurve marktype x color 0 0 0 label : {0}\n".format(self.whiskercomponents[0]))


    def plot_whisker(self):
        ct = 0.5
        # for x in range(len(self.whisker_y_vals)): # Doesn't work when a value of x is skipped: like 1:10, 2:20, 3:30, 4:??, 5:50. However, works better if we are plotting a value per hour-of-the-day, for example, where we need to plot every value in the range(0, 24). This version forces us to recognize missing values, if any, since the code will throw an error.
        for x in self.whisker_x_vals: # Works better when x doesn't have a specific range. For example, if the xaxis is ASNs and yaxis contains a value per ASN, x is not going to range neatly from 0 to N.
            mark_idx = 0
            if (len(self.whiskercomponents) == 1):
                # print "{0} {1} {2}\n".format(ct, x, self.whisker_y_vals[x])
                # print "Printing from plot_whisker"
                # print x, self.whisker_y_vals[x]
                self.fp.write("newcurve marktype x color 0 0 0 linetype solid y_epts {0} {1} {2} {3}\n".format(ct, self.whisker_y_vals[x][0], self.whisker_y_vals[x][1], self.whisker_y_vals[x][2]))
            else:
                if (len(self.whiskercomponents) == 2):
                    deltas = [0.1, -0.1]
                elif (len(self.whiskercomponents) == 3):
                    deltas = [0.2, 0, -0.2]
                for i in range(len(self.whiskercomponents)):
                    # print x
                    # print self.whisker_y_vals[i][x]
                    y_val = self.whisker_y_vals[i][x]
                    self.fp.write("newcurve marktype {0} color 0 0 0 linetype solid y_epts {1} {2} {3} {4}\n".format(self.whisker_marktypes[mark_idx], ct - deltas[mark_idx], y_val[0], y_val[1], y_val[2]))
                    mark_idx += 1

            ct += 1


class CDF_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))

        # self.sorted_cdf_pts_fnames = []
        self.cdfcolors = [
        # "color 0.8 0.2 0.2",
        # "color 0.2 0.8 0.2",
        # "color 0.2 0.2 0.8",
        # "color 0.7 0.7 0.7",
        # "color 0.2 0.8 0.7",
        # "color 0.7 0.2 0.7",
        # "color 0.7 0.7 0.2",
        # "color 0 0 0",

        # For talk
        "color 0.1 0.65 0.65",
        "color 0.65 0.65 0.2",
        "color 0.65 0.3 0.65",
        "color 0.8 0 0",
        "color 0.2 0.8 0.1",
        "color 0.2 0.2 0.8",
        "color 0.3 0.3 0.3",

        # For paper?
        # "color 0.6 0.9 0.6",
        # "color 0.3 0.6 0.9",
        # "color 0.8 0.2 0.2",
        # "color 0.8 0.8 0.8",
        # "color 0 0 0",

        # Dunno for what...
        # "color 0.2 0.8 0.7",
        # "color 0.4 0.9 0.2",
        # "color 0.9 0 0.9",
        ]
        self.cdflinetypes = [
        "solid",
        "dotted",
        "dotdash",
        # "dotted",
        # "solid",
        "dashed",
        # "longdash",
        # "dotdash",
        # "dotdotdash",
        # "dotdotdashdash",
        # "dotted"
        ]

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label fontsize 13\n'
        # conf += 'hash_labels fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        conf += 'min 0\n'
        return conf

    def get_cdf_fnames(self, cdf_fnames_list):
        self.cdf_fnames_list = cdf_fnames_list

    def calc_n_vals_per_fname(self):
        self.n_vals = []
        for cdf_inp_fname in self.cdf_fnames_list:
            ip_fp = open(cdf_inp_fname, "r")
            n_vals = 0
            for line in ip_fp:
                n_vals += 1
            self.n_vals.append(n_vals)
            ip_fp.close()

    def write_cdf_pts(self, fname):
        ip_fp = open(fname, "r")
        vals = []
        for line in ip_fp:
            val = float(line[:-1])
            # NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful
            val = round(val, 4)
            vals.append(val)

        vals.sort()

        n_vals = len(vals)
        n_curr = 0 # Number of vals <= curr value of x (CDF is P(X <= x))

        print "xlog: {0}".format(self.xlog)
        print "ylog: {0}".format(self.ylog)

        if ( (self.xlog == False) and (self.ylog == False) ):
            self.fp.write("0 0\n")

        prev = vals[0]

        prev_n_curr = 0

        for val in vals:
            if val != prev:
                if ( (self.xlog == False) or (prev > 0) ):
                    self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (prev_n_curr*1.0)/n_vals))
                    self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (n_curr*1.0)/n_vals))
                    prev_n_curr = n_curr

            prev = val
            n_curr += 1

        self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (prev_n_curr*1.0)/n_vals))
        self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (n_curr*1.0)/n_vals))

        ip_fp.close()


    def cdf(self, label=""):
        # Handle special case where we are only plotting a single file
        if (len(self.cdf_fnames_list) == 1):
            # Use black color
            self.fp.write("newcurve marktype none linethickness 2 linetype solid {0}\n".format(self.cdfcolors[-1]))
            if (label != ""):
                self.fp.write("label : {0}\n".format(label))
            self.fp.write("pts\n")
            self.write_cdf_pts(self.cdf_fnames_list[0])

        else:
            i = 0
            j = 0
            for cdf_inp_fname in self.cdf_fnames_list:
                self.fp.write("newcurve marktype none linethickness 2 linetype {0} {1}\n".format(self.cdflinetypes[j], self.cdfcolors[i]))
                if (label != ""):
                    self.fp.write("label : {0}\n".format(label))
                self.fp.write("pts\n")
                self.write_cdf_pts(cdf_inp_fname)

                i += 1
                if (i == len(self.cdfcolors)):
                    i = 0

                j += 1
                if (j == len(self.cdflinetypes)):
                    j = 0


class PDF_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False

        # self.sorted_pdf_pts_fnames = []
        self.pdfcolors = [
        # "color 0.8 0.2 0.2",
        # "color 0.2 0.8 0.2",
        # "color 0.2 0.2 0.8",
        # "color 0.7 0.7 0.7",
        # "color 0.2 0.8 0.7",
        # "color 0.7 0.2 0.7",
        # "color 0.7 0.7 0.2",
        # "color 0 0 0",

        # For talk
        "color 0.1 0.65 0.65",
        "color 0.65 0.65 0.2",
        "color 0.65 0.3 0.65",
        "color 0.8 0 0",
        "color 0.2 0.8 0.1",
        "color 0.2 0.2 0.8",
        "color 0.3 0.3 0.3",

        # For paper?
        # "color 0.6 0.9 0.6",
        # "color 0.3 0.6 0.9",
        # "color 0.8 0.2 0.2",
        # "color 0.8 0.8 0.8",
        # "color 0 0 0",

        # Dunno for what...
        # "color 0.2 0.8 0.7",
        # "color 0.4 0.9 0.2",
        # "color 0.9 0 0.9",
        ]
        self.pdfmarktypes = [
        "x",
        "diamond",
        "box",
        # "dotted",
        # "solid",
        "circle",
        "cross",
        # "longdash",
        # "dotdash",
        # "dotdotdash",
        # "dotdotdashdash",
        # "dotted"
        ]

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label fontsize 13\n'
        # conf += 'hash_labels fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        conf += 'min 0\n'
        return conf

    def get_pdf_fnames(self, pdf_fnames_list):
        self.pdf_fnames_list = pdf_fnames_list

    def calc_n_vals_per_fname(self):
        self.n_vals = []
        for pdf_inp_fname in self.pdf_fnames_list:
            ip_fp = open(pdf_inp_fname, "r")
            n_vals = 0
            for line in ip_fp:
                n_vals += 1
            self.n_vals.append(n_vals)
            ip_fp.close()

    def write_pdf_pts(self, fname):
        ip_fp = open(fname, "r")
        vals = []
        for line in ip_fp:
            val = float(line[:-1])
            # NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful
            val = round(val, 4)
            vals.append(val)

        vals.sort()

        n_vals = len(vals)
        n_curr = 0 # Number of vals == curr value of x (PDF is P(X == x))

        print "xlog: {0}".format(self.xlog)
        print "ylog: {0}".format(self.ylog)

        # PDF doesn't need to start off at 0 0
        # if ( (self.xlog == False) and (self.ylog == False) ):
        #     self.fp.write("0 0\n")
        prev = vals[0]

        prev_n_curr = 0

        for val in vals:
            if val != prev:
                if ( (self.xlog == False) or (prev > 0) ):
                    self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (n_curr*1.0)/n_vals))
                    n_curr = 0

            prev = val
            n_curr += 1

        self.fp.write("{0:.4f} {1:.4f}\n".format(prev, (n_curr*1.0)/n_vals))

        ip_fp.close()


    def pdf(self, label=""):
        # Handle special case where we are only plotting a single file
        if (len(self.pdf_fnames_list) == 1):
            # Use black color
            # self.fp.write("newcurve marktype none linethickness 2 linetype solid {0}\n".format(self.pdfcolors[-1]))
            self.fp.write("newcurve marktype x {0} linetype none\n".format(self.pdfcolors[-1]))
            if (label != ""):
                self.fp.write("label : {0}\n".format(label))
            self.fp.write("pts\n")
            self.write_pdf_pts(self.pdf_fnames_list[0])

        else:
            i = 0
            j = 0
            for pdf_inp_fname in self.pdf_fnames_list:
                self.fp.write("newcurve marktype {0} {1} linetype none\n".format(self.pdfmarktypes[j], self.pdfcolors[i]))
                if (label != ""):
                    self.fp.write("label : {0}\n".format(label))
                self.fp.write("pts\n")
                self.write_pdf_pts(pdf_inp_fname)

                i += 1
                if (i == len(self.pdfcolors)):
                    i = 0

                j += 1
                if (j == len(self.pdfmarktypes)):
                    j = 0


class CCDF_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False

        # self.sorted_cdf_pts_fnames = []
        self.cdfcolors = [
        # "color 0.8 0.2 0.2",
        # "color 0.2 0.8 0.2",
        # "color 0.2 0.2 0.8",
        # "color 0.7 0.7 0.7",
        # "color 0.2 0.8 0.7",
        # "color 0.7 0.2 0.7",
        # "color 0.7 0.7 0.2",
        # "color 0 0 0",

        # For talk
        "color 0.1 0.65 0.65",
        "color 0.65 0.65 0.2",
        "color 0.65 0.3 0.65",
        "color 0.8 0 0",
        "color 0.2 0.8 0.1",
        "color 0.2 0.2 0.8",
        "color 0.3 0.3 0.3",

        # For paper?
        # "color 0.6 0.9 0.6",
        # "color 0.3 0.6 0.9",
        # "color 0.8 0.2 0.2",
        # "color 0.8 0.8 0.8",
        # "color 0 0 0",

        # Dunno for what...
        # "color 0.2 0.8 0.7",
        # "color 0.4 0.9 0.2",
        # "color 0.9 0 0.9",
        ]
        self.cdflinetypes = [
        "solid",
        "dotted",
        "dotdash",
        # "dotted",
        # "solid",
        "dashed",
        # "longdash",
        # "dotdash",
        # "dotdotdash",
        # "dotdotdashdash",
        # "dotted"
        ]

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label fontsize 13\n'
        # conf += 'hash_labels fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        # conf += 'max 1\n'
        conf += 'no_auto_hash_labels\n'
        # conf += 'no_auto_hash_marks\n'
        conf += 'hash_at 1\n'
        conf += 'hash_label at 1 : 1\n'
        conf += 'hash_at 0.1\n'
        conf += 'hash_label at 0.1 : 0.1\n'
        conf += 'hash_at 0.01\n'
        conf += 'hash_label at 0.01 : 0.01\n'
        conf += 'hash_at 0.001\n'
        conf += 'hash_label at 0.001 : 0.001\n'
        return conf

    def get_cdf_fnames(self, cdf_fnames_list):
        self.cdf_fnames_list = cdf_fnames_list

    def calc_n_vals_per_fname(self):
        self.n_vals = []
        for cdf_inp_fname in self.cdf_fnames_list:
            ip_fp = open(cdf_inp_fname, "r")
            n_vals = 0
            for line in ip_fp:
                n_vals += 1
            self.n_vals.append(n_vals)
            ip_fp.close()

    def write_cdf_pts(self, fname):
        ip_fp = open(fname, "r")
        vals = []
        for line in ip_fp:
            val = float(line[:-1])
            # # NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful.
            # NOTE: If I'm drawing a CCDF, it's likely because I want to see very small values of y. Thus, I won't round.
            # val = round(val, 4)
            vals.append(val)

        vals.sort()

        n_vals = len(vals)
        n_curr = 0
        # Number of vals > curr value of x (CCDF is P(X > x)
        # Alternately, CCDF is: 1 - P(X <= x)

        print "xlog: {0}".format(self.xlog)
        print "ylog: {0}".format(self.ylog)

        if ( (self.xlog == False) and (self.ylog == False) ):
            self.fp.write("0 1\n") # In CCDF, initially all points are > 0

        prev = vals[0]

        prev_n_curr = 0

        for val in vals:
            if val != prev:
                if ( (self.xlog == False) or (prev > 0) ):
                    # print val, prev_n_curr, n_curr
                    self.fp.write("{0:.8f} {1:.8f}\n".format(prev, 1 - ((prev_n_curr*1.0)/n_vals) ))
                    self.fp.write("{0:.8f} {1:.8f}\n".format(prev, 1 - ((n_curr*1.0)/n_vals) ))
                    prev_n_curr = n_curr
            prev = val
            n_curr += 1

        self.fp.write("{0:.8f} {1:.8f}\n".format(prev, 1 - ((prev_n_curr*1.0)/n_vals) ))
        if (self.ylog == False):
            self.fp.write("{0:.8f} {1:.8f}\n".format(prev, 1 - ((n_curr*1.0)/n_vals) ))

        ip_fp.close()

    def cdf(self, label=""):
        # Handle special case where we are only plotting a single file
        if (len(self.cdf_fnames_list) == 1):
            # Use black color
            self.fp.write("newcurve marktype none linethickness 2 linetype solid {0}\n".format(self.cdfcolors[-1]))
            if (label != ""):
                self.fp.write("label : {0}\n".format(label))
            self.fp.write("pts\n")
            self.write_cdf_pts(self.cdf_fnames_list[0])

        else:
            i = 0
            j = 0
            for cdf_inp_fname in self.cdf_fnames_list:
                self.fp.write("newcurve marktype none linethickness 2 linetype {0} {1}\n".format(self.cdflinetypes[j], self.cdfcolors[i]))
                if (label != ""):
                    self.fp.write("label : {0}\n".format(label))
                self.fp.write("pts\n")
                self.write_cdf_pts(cdf_inp_fname)

                i += 1
                if (i == len(self.cdfcolors)):
                    i = 0

                j += 1
                if (j == len(self.cdflinetypes)):
                    j = 0


class Weighted_CDF_Jgraph(Jgraph):
    def init_jgraph(self, jgr_fname):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        # self.fp.write("\nnewgraph clip\n\n")
        self.fp.write("\nnewgraph\n\n") # Not going to clip by default
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False

        # self.sorted_cdf_pts_fnames = []
        self.cdfcolors = [
        # "color 0.8 0.2 0.2",
        # "color 0.2 0.8 0.2",
        # "color 0.2 0.2 0.8",
        # "color 0.7 0.7 0.7",
        # "color 0.2 0.8 0.7",
        # "color 0.7 0.2 0.7",
        # "color 0.7 0.7 0.2",
        # "color 0 0 0",

        # For paper
        # "color 0.8 0.2 0.2",
        # "color 0.3 0.6 0.9",
        # "color 0.6 0.9 0.6",
        # "color 0.8 0.8 0.8",
        # "color 0 0 0",

        # For talk
        "color 0.1 0.65 0.65",
        "color 0.65 0.65 0.2",
        "color 0.65 0.3 0.65",
        "color 0.8 0 0",
        "color 0.2 0.8 0.1",
        "color 0.2 0.2 0.8",
        "color 0.8 0.8 0.8",

        # For DE ASes
        # "color 0.65 0.65 0.2",
        # "color 0.8 0 0",
        # "color 0.2 0.2 0.8",
        # "color 0.8 0.8 0.8",
        # "color 0.3 0.95 0.6",
        ]

        self.cdflinetypes = [
        # For paper
        "solid",
        "dotted",
        "dotdash"

        # For talk
        # "solid",
        # "solid",
        # "solid",
        # "dotted",
        # "solid",
        # "dashed",
        # "longdash",
        # "dotdash",
        # "dotdotdash",
        # "dotdotdashdash",
        # "dotted"
        ]

    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        # conf += 'label fontsize 13\n'
        # conf += 'hash_labels fontsize 11\n\n'
        conf += 'label : {0}\n'.format(yaxis_label)
        conf += 'min 0\n'
        return conf

    def get_cdf_fnames(self, cdf_fnames_list):
        self.cdf_fnames_list = cdf_fnames_list

    def calc_total_vals_per_fname(self):
        self.total_vals = []
        for cdf_inp_fname in self.cdf_fnames_list:
            ip_fp = open(cdf_inp_fname, "r")
            total = 0
            for line in ip_fp:
                val = float(line[:-1])
                # NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful
                val = round(val, 4)
                total += val
            self.total_vals.append(total)
            ip_fp.close()

    def write_cdf_pts(self, fname):
        ip_fp = open(fname, "r")
        vals = []
        for line in ip_fp:
            val = float(line[:-1])
            # NOTE: If I have very tiny values (<0.0001), then rounding to 4 significant digits would be bad. Be careful
            val = round(val, 4)
            vals.append(val)

        vals.sort()

        # In a weighted graph, n_vals = val1 + val2 + val3 + ...
        total = 0
        for val in vals:
            total += val

        print "Total : {0}".format(total)

        n_curr = 0 # Number of vals <= curr value of x (CDF is P(X <= x))

        if ( (self.xlog == False) and (self.ylog == False) ):
            self.fp.write("0 0\n")

        prev = vals[0]

        prev_n_curr = 0
        prev_y_val = 0

        for val in vals:
            if val != prev:
                if ( (self.xlog == False) or (prev > 0) ):
                    x_val = prev
                    this_y_val = (prev_y_val + (n_curr - prev_n_curr)*prev)*1.0
                    self.fp.write("{0:.4f} {1:.4f}\n".format(x_val, prev_y_val/total))
                    self.fp.write("{0:.4f} {1:.4f}\n".format(x_val, this_y_val/total))
                    prev_y_val = this_y_val
                    prev_n_curr = n_curr

            prev = val
            n_curr += 1

        x_val = prev
        this_y_val = (prev_y_val + (n_curr - prev_n_curr)*prev)*1.0
        self.fp.write("{0:.4f} {1:.4f}\n".format(x_val, prev_y_val/total))
        self.fp.write("{0:.4f} {1:.4f}\n".format(x_val, this_y_val/total))

        ip_fp.close()

    def cdf(self, label=""):
        # Handle special case where we are only plotting a single file
        if (len(self.cdf_fnames_list) == 1):
            # Use black color
            self.fp.write("newcurve marktype none linethickness 2 linetype solid {0}\n".format(self.cdfcolors[-1]))
            if (label != ""):
                self.fp.write("label : {0}\n".format(label))
            self.fp.write("pts\n")
            self.write_cdf_pts(self.cdf_fnames_list[0])

        else:
            i = 0
            j = 0
            for cdf_inp_fname in self.cdf_fnames_list:
                self.fp.write("newcurve marktype none linethickness 2 linetype {0} {1}\n".format(self.cdflinetypes[j], self.cdfcolors[i]))
                if (label != ""):
                    self.fp.write("label : {0}\n".format(label))
                self.fp.write("pts\n")
                self.write_cdf_pts(cdf_inp_fname)

                i += 1
                if (i == len(self.cdfcolors)):
                    i = 0

                j += 1
                if (j == len(self.cdflinetypes)):
                    j = 0


def usage(args):
    sys.stderr.write("usage: {0} -i <inpfiles> -o <opfile> -x <xaxis label> -y <yaxis label>\n".format(args[0]))

if __name__=="__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:o:x:y:u", ["inpfiles=", "opfile=", "xlabel=", "ylabel=", "usage"])

    except getopt.GetoptError as err:
        print str(err)
        usage(sys.argv)
        sys.exit(1)

    inpfiles = None
    xlabel = None
    ylabel = None

    for o, a in opts:
        if o in ("-i", "--inpfiles"):
            inpfiles = a
        elif o in ("-o", "--opfile"):
            op_fname = a
        elif o in ("-x", "--xlabel"):
            xlabel = a
            print xlabel
        elif o in ("-y", "--ylabel"):
            ylabel = a
            print ylabel
        elif o in ("-u", "--usage"):
            usage(sys.argv)
            sys.exit(1)
        else:
            assert False, "unhandled option"

    # The input files for the plots are listed in the filename held in the var: inpfiles.
    inp_fnames = []
    inpfiles_fp = open(inpfiles, "r")
    for f in inpfiles_fp:
        inp_fnames.append(f[:-1])

    j = Jgraph()
    j.init_jgraph(op_fname)
    j.set_xlog()
    j.xaxis(xlabel)
    j.yaxis(ylabel)
    j.get_cdf_fnames(inp_fnames)
    j.cdf()
