
import sys
import getopt

# Import utils script for inheriting the Jgraph class
zeusping_utils_path = sys.path[0][0:(sys.path[0].find("zeusping") + len("zeusping"))]
sys.path.append(zeusping_utils_path + "/utils")
import jgraph

# For executing commands
import os
import shlex, subprocess
import time

is_talk = 0

class TimeSeries_Scatter(jgraph.Scatter_Jgraph):
    def init_jgraph(self, jgr_fname, title=None):
        self.op_fname = jgr_fname
        self.fp = open(self.op_fname, "w")
        self.fp.write("\nnewgraph\n\n")  # No clip so that I won't miss values being outside
        self.curvenum = 0
        self.remove_xoffset = False
        self.xlog = False
        self.ylog = False
        if (title != None):
            self.fp.write("\n title : {0}\n\n".format(title))
        # self.scatter_marktypes = ['x', 'diamond', 'box', 'circle', 'cross', 'triangle']
        self.scatter_marktypes = ['cross', 'circle', 'triangle', 'diamond', 'x', 'box', ]
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
        "color 0.2 0.8 0.1",
        "color 0.8 0 0",
        "color 0.2 0.2 0.8",
        "color 0.65 0.65 0.2",
        "color 0.1 0.65 0.65",
        "color 0.65 0.3 0.65",
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

        
    def set_to_plot_var(self, to_plot_var):
        self.to_plot_var = to_plot_var
        
    
    def config_xaxis(self, xaxis_label):
        conf = '\n'
        if (self.xlog):
            conf += 'log\n'
        conf += 'size 4\n'

        if (is_talk == 1):
            conf += "gray 0.9\n"
            # conf += 'label fontsize 20\n'
            # conf += 'hash_labels fontsize 16\n\n'
        else:
            conf += 'label : {0}\n'.format(xaxis_label)

        conf += 'no_auto_hash_labels\n'
        conf += 'no_auto_hash_marks\n'

        for h_idx in range(14, 575, 24):
            conf += 'hash_at {0}\n'.format(h_idx)
            conf += 'hash_label at {0} : {1}\n'.format(h_idx, ((h_idx - 14)%144)/6 )
        
        return conf


    def config_yaxis(self, yaxis_label):
        conf = '\n'
        if (self.ylog):
            conf += 'log\n'
        conf += 'size 3\n'
        if (is_talk == 1):
            conf += "gray 0.9\n"
            # conf += 'label fontsize 20\n'
            # conf += 'hash_labels fontsize 16\n\n'
        else:
            conf += 'label : {0}\n'.format(yaxis_label)            
            # conf += 'label fontsize 13\n'
            # conf += 'hash_labels fontsize 11\n\n'


        if self.to_plot_var == 'resp':

            conf += 'no_auto_hash_labels\n'
            conf += 'no_auto_hash_marks\n'

            conf += 'hash_at 972000\n'
            conf += 'hash_label at 972000 : 972K\n'

            conf += 'hash_at 973000\n'
            conf += 'hash_label at 973000 : 973K\n'

            conf += 'hash_at 974000\n'
            conf += 'hash_label at 974000 : 974K\n'

            conf += 'hash_at 975000\n'
            conf += 'hash_label at 975000 : 975K\n'

            conf += 'hash_at 976000\n'
            conf += 'hash_label at 976000 : 976K\n'

            conf += 'hash_at 977000\n'
            conf += 'hash_label at 977000 : 977K\n'

        elif self.to_plot_var == 'newresp_dropout':
            pass

        return conf


    def draw_cdt_gridlines(self):
        # cdt_idx = 5.0 # At 5 AM UTC, it's midnight CDT (which is the same as EST)
        for cdt_idx in range(14 + 5*6, 575, 24*6):
            # Add a grid line for CDT
            if is_talk == 1:
                self.fp.write('newcurve marktype none linetype dashed color gray 0.9 pts {0} 0.0005 {0} 0.05\n'.format(cdt_idx) )
            else:
                self.fp.write('newcurve marktype none linetype dashed color 0 0 0 pts {0} 800 {0} 2200\n'.format(cdt_idx) )                
            # cdt_idx += 24
        
    
    def plot_scatter(self, label=""):

        scatter_fname = self.scatter_fnames_list
        scatter_fp = open(scatter_fname, 'r')

        for line in scatter_fp:
            parts = line.strip().split()
            tstamp = int(parts[0].strip() )

            x_val = (tstamp - 1565559600)/float(600)



            if self.to_plot_var == 'resp':
                n_r = int(parts[1].strip() )
                self.fp.write("newcurve marktype diamond color 0 0 0.8 linetype none pts {0} {1}\n".format(x_val, n_r) )

            elif self.to_plot_var == 'newresp_dropout':
                n_d = int(parts[2].strip() )
                self.fp.write("newcurve marktype x color 0.8 0 0 linetype none pts {0} {1}\n".format(x_val, n_d) )
                n_n = int(parts[3].strip() )
                self.fp.write("newcurve marktype cross color 0 0.8 0 linetype none pts {0} {1}\n".format(x_val, n_n) )

            elif self.to_plot_var == 'p_d':
                n_r = int(parts[1].strip() )
                n_d = int(parts[2].strip() )
                p_d = float(n_d)/n_r
                self.fp.write("newcurve marktype x color 0.8 0 0 linetype none pts {0} {1}\n".format(x_val, p_d) )

                
def usage(args):
    sys.stderr.write("usage: {0} -i <inpfiles> -o <opfile> -x <xaxis label> -y <yaxis label>\n".format(args[0]))


if __name__=="__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:o:x:y:t:p:u", ["inpfile=", "opfile=", "xlabel=", "ylabel=", "title=", "to_plot_var", "usage"])

    except getopt.GetoptError as err:
        print str(err)
        usage(sys.argv)
        sys.exit(1)

    inp_fname = None
    xlabel = None
    ylabel = None
    title = None
    to_plot_var = 'newresp_dropout'

    for o, a in opts:
        if o in ("-i", "--inpfiles"):
            inp_fname = a
        elif o in ("-o", "--opfile"):
            op_fname = a
        elif o in ("-x", "--xlabel"):
            xlabel = a
            print xlabel
        elif o in ("-y", "--ylabel"):
            ylabel = a
            print ylabel
        elif o in ("-t", "--title"):
            title = a
        elif o in ("-p", "--to_plot_var"):
            to_plot_var = a
        elif o in ("-u", "--usage"):
            usage(sys.argv)
            sys.exit(1)
        else:
            assert False, "unhandled option"


    # inp_fp = open(inp_fname, 'r')

    xlabel = "Time"
    
    if to_plot_var == 'newresp_dropout':
        ylabel = "Events"
    elif to_plot_var == 'resp':
        ylabel = "Responsive addresses"
    elif to_plot_var == 'p_d':
        ylabel = "Probability of dropout"

    j = TimeSeries_Scatter()
    if (title == None):
        j.init_jgraph(op_fname)
    else:
        j.init_jgraph(op_fname, title=title)

    j.set_to_plot_var(to_plot_var)
    
    if to_plot_var == 'p_d':
        j.set_ylog()

    j.xaxis(xlabel)
    j.yaxis(ylabel)
    # inp_fname is the file containing dropouts, newresp events    
    j.get_scatter_fnames(inp_fname)
    # j.draw_cdt_gridlines()    
    j.plot_scatter()
    j.exec_jgraph()
    bp_path = "ramapad@bluepill.cs.umd.edu:~/public_html/dhcp_paper/atlas_ripe/"
    j.scp_to_bp(bp_path)
    


