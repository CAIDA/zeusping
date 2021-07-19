
import sys

fqdn = sys.argv[1]
alias = sys.argv[2]

reqd_addr_cnts = ["pinged_addr_cnt", "echoresponse_addr_cnt", "responsive_addr_cnt", "dropout_addr_cnt", "antidropout_addr_cnt", "disrupted_addr_cnt", "antidisrupted_addr_cnt"] # For testing swift_process_round_wandiocat.py by comparing against swift_process_round_simple.py
# reqd_addr_cnts = ["pinged_addr_cnt", "echoresponse_addr_cnt",  "disrupted_addr_cnt", "antidisrupted_addr_cnt"]
# reqd_addr_cnts = ["echoresponse_addr_cnt"]

query = ''

for addr_cnt in reqd_addr_cnts:

    this_query = '''
      alias(
        keepLastValue(
          sumSeries(
            {0}.{1},
          )
        ),
        "{2} {1}"
      )'''.format(fqdn, addr_cnt, alias)

    query += '{0},'.format(this_query)


sys.stdout.write("{0}\n".format(query) )
