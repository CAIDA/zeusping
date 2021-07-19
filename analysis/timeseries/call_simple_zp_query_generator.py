
import sys
import os
import subprocess

fqdn = sys.argv[1]
alias = sys.argv[2]

aggr = 'US_ME'

aggr_to_asns = {

    'VE' : ['8048', '8053', '27889', '21826', '11562'], # VE
    'VE_Tachira' : ['8048', '21826', '11562'], # VE Tachira
    'VE_Cojedes' : ['8048', '21826', '27889'], # VE Cojedes
    'NG' : ['29091', '29465', '35074', '36920', '36923', '37282', '37340'], # NG
    'NG_FCT' : ['29091', '29465', '37282', '37340'], # NG Federal Capital Territory
    'KZ' : ['9198', '41798', '41124', '39824', '35104', '21299'], # Kazakhstan
    'GH' : ['29614', '327814', '328439', '328571', '35091', '37030', '37350', '37623'], # GH
    'GH_Ashanti' : ['29614', '327814', '35091'], # GH Ashanti
    'TG' : ['24691', '36924'], # TG

    'PS' : ['12975', '15975', '51407', '51336'], # PS
    'IL' : ['12400', '8551', '1680', '12849', '47956', '9116'], # IL
    'JO' : ['8376', '48832', '9038', '47887', '44702', '50670', '8697'], # JO
    'LB' : ['9051', '31126', '39010', '197674', '42334'], # LB
    'CO' : ['3816', '10620', '19429', '13489', '27831', '14080', '10299'], # CO
    # asn_list = ['58224', '197207', '44244', '31549', '16322', '12880', '56402', '57218', '39501', '42337'], # IR
    # asn_list = ['39501', '42337', '44208', '58224'], # IR Fars
    # asn_list = ['39501', '58224'], # IR Qom
    'US_ME' : ['7922', '11351', '13977', '5760'], # Maine
    # asn_list = ['209', '22773', '53508', '7922'], # Arkansas
    # asn_list = ['20115', '22773', '5009', '7922'], # Louisiana
    # asn_list = ['20115', '22773'], # Louisiana St James
    # asn_list = ['5650', '33363', '209', '23089', '22773', '7922'], # Florida
    # asn_list = ['209', '22773', '7922'], # Arkansas Pulaski
    'US_TX' : ['7922', '209', '7459', '16591', '5650', '20115', '7029', '17306'], # Texas
    'US_TX_Dallas' : ['11427', '5650', '20115'], # Texas Dallas 
    'US_TX_Anderson' : ['209', '7029'], # Texas Anderson
    'US_TX_Harris' : ['7922', '209', '5650'], # Texas Harris 
    'US_TX_Houston' : ['7029'], # Texas Houston
    'US_TX_TomGreen' : ['209', '5650', '17306'], # Texas Tom Green
    'US_TX_Nueces' : ['11427', '7459'], # Texas Nueces
    'US_TX_Montgomery' : ['20115', '209', '7922'], # Texas Montgomery
    'US_TX_Galveston' : ['5650', '7922'], # Texas Galveston
    'US_TX_Libery' : ['5650', '7922'], # Texas Liberty
    'US_TX_Hidalgo' : ['11427', '209'], # Texas Hidalgo
    'US_TX_Bee' : ['11427'], # Texas Bee
    'US_TX_Atascosa' : [''], # Texas Atascosa has no data, really..
    # asn_list = ['7922', '33363', '209', '23089', '22773', '5650'], # Florida
    # asn_list = ['7922', '33363', '209', '22773', '5650'], # Florida Collier
    'US_CA' : ['46375', '22773', '33363', '7065', '20001', '20115', '7922', '5650'], # California
    # asn_list = ['46375', '7922'], # California Alameda county
    # asn_list = ['46375', '7065', '20001', '20115', '7922', '5650'], # California Contra Costa county
    'US_CA_Monterey' : ['20115', '7922'], # California Monterey county
    # asn_list = ['20115', '7922', '5650'], # California San Luis Obispo county
    # asn_list = ['7065', '20115', '7922', '5650'], # California San Joaquin county
    'US_CA_SanDiego' : ['20001', '22773'], # California San Diego county
    'US_CA_LosAngeles' : ['20001', '5650', '20115'], # California Los Angeles county
    # asn_list = ['20001', '5650', '20115'], # California Riverside county
    # asn_list = ['20001', '20115', '7922', '5650'], # California Fresno county
    # asn_list = ['7065', '7922'], # California Tuolumne county
    # asn_list = ['7922', '5650', '7065'], # California Yuba county
    # asn_list = ['11351', '13977', '5760', '7922'], # Maine
    # asn_list = ['11776', '14291', '701', '7922'], # Maryland
    # asn_list = ['9988', '136255', '132167', '133385', '18399', '58952', '136210', '133524', '135300', '136480'], # Myanmar
}

py_cmd = "python simple_zp_query_generator.py {0} {1}".format(fqdn, alias)
print subprocess.check_output(py_cmd, shell=True)

for asn in aggr_to_asns[aggr]:
    py_cmd = "python simple_zp_query_generator.py {0}.asn.{1} {2}_AS{1}".format(fqdn, asn, alias)
    # sys.stderr.write("{0}\n".format(py_cmd) )
    # os.system(py_cmd)
    print subprocess.check_output(py_cmd, shell=True)
