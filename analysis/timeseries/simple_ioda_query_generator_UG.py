
import sys

reqd_asns = {
    # '20294' : 'MTN',
    '29039' : 'Africa Online Uganda',
    # '36991' : 'Africell Uganda',
    # '36997' : 'Infocom',
    # '37063' : 'Roke',
    '37075' : 'Airtel Uganda',
    '36977' : 'Airtel Uganda',
    # '21491' : 'Uganda Telecom',
    # '328015' : 'Sombha',
    # '328198' : 'Blue Crane',
    # '37122' : 'Smile',
    # '37113' : 'Tangerine',
    # '327687' : 'RENU',
}

sig = sys.argv[1]
query = ''
for asn, asname in reqd_asns.items():

    if sig == 'active':
        this_query = '''
        alias(
          removeEmpty(
            normalize(
              sumSeries(
                keepLastValue(
                  active.ping-slash24.asn.{0}.probers.team-1.caida-sdsc.*.up_slash24_cnt,
                  1
                )
              )
            )
          ),
          "{1} (AS{0})"
        )'''.format(asn, asname)
    elif sig == 'bgp':
        this_query = '''
        alias(
            normalize(
             bgp.prefix-visibility.asn.{0}.v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt
            ),
          "{1} (AS{0})"
        )'''.format(asn, asname)

    query += '{0},'.format(this_query)


sys.stdout.write("{0}\n".format(query[:-1]) )
