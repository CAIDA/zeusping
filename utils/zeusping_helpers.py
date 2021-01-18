
import shlex
import subprocess


def load_idx_to_dicts(loc_fname, idx_to_loc_fqdn, idx_to_loc_name, idx_to_loc_code, ctry_code_to_fqdn=None):
    read_cmd = 'zcat {0}'.format(loc_fname)
    args = shlex.split(read_cmd)

    try:
        proc = subprocess.Popen(read_cmd, stdout=subprocess.PIPE, bufsize=-1, shell=True, executable='/bin/bash')
    except:
        sys.stderr.write("read cmd failed for {0}; exiting\n".format(read_cmd) )

    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            parts = line.decode().strip().split(',')
            idx = parts[0].strip()
            fqdn = parts[1].strip()
            idx_to_loc_fqdn[idx] = fqdn
            loc_name = parts[2][1:-1] # Get rid of quotes
            idx_to_loc_name[idx] = loc_name
            loc_code = parts[3]
            idx_to_loc_code[idx] = loc_code

            if ctry_code_to_fqdn is not None:
                ctry_code_to_fqdn[loc_code] = fqdn

