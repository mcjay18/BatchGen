#!/usr/bin/env python3

"""
Script to break a list of MCD groups into batches of X size for migration tools

Script will take timezone of state capital and break into regional group

Version:
    1.0: Initial script
"""

import logging
import configparser
import pandas
import argparse
import re
import csv
import json




# Logging
def init_logging():
    #    'Initialize Logging Globally'

    # Specify our log format for handlers
    log_format = logging.Formatter('%(asctime)s %(name)s:%(levelname)s: %(message)s')

    # Get the root_logger to attach log handlers to
    root_logger = logging.getLogger()

    # Set root logger to debug level always
    # Control log levels at log handler level
    root_logger.setLevel(logging.DEBUG)

    # Console Handler (always use log_level)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(log_format)
    root_logger.addHandler(ch)

    # Logfile Handler
    fh = logging.FileHandler(log_file)

    # Always log at INFO or below
    if log_level < logging.INFO:
        fh.setLevel(log_level)
    else:
        fh.setLevel(logging.INFO)

    # Attach logfile handler
    fh.setFormatter(log_format)
    root_logger.addHandler(fh)


# conversion for time converts from seconds
# def convert(n):
#     return str(datetime.timedelta(seconds=n))

def divide_chunks(l, n):
    '''
    Devices a list into chunks
    :param l: initial list
    :param n: chunck size
    :return: list of chunks
    '''
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def storenum_shorten(num):
    srtnum = abs(int(num))
    return srtnum

def dict_filter(it, *keys):
    for d in it:
        yield dict((k, d[k]) for k in keys)

def filemake(name,chunked):
    chunknum = 1
    for chunk in chunked:
        fnname = (f'{name}_{chunknum}')
        for chu in chunk:
            chu.pop('zone')
        # print(chunk)

        df = pandas.DataFrame(chunk)
        print(f'[W] Writing {fnname}')
        df.to_csv(fnname + ".csv", index=False)
        df.to_excel(fnname + ".xlsx", index=False)
        chunknum += 1


# Main Logging Start ####
logger = logging.getLogger('BatchGen')
# Main logging End ####


# Parse Command Line Arguments
parser = argparse.ArgumentParser(
    description='Script to break a list of MCD groups into batches of X size for migration tools',
    epilog='Contact Jay McNealy with questions or feature requests.')


parser.add_argument("-i", help="Input file", type=str, required=True)
parser.add_argument("-b", help="Batch number", type=str, required=True)
parser.add_argument("-p", help="Pod number", type=str, required=True)
parser.add_argument("-c", help="chunk size", type=int, default=50)
parser.add_argument("-t", help="Split on chunk and timezone ",action="store_true" )

# group = parser.add_mutually_exclusive_group()
# group.add_argument("-S", help="Single Get run", action="store_true")
# group.add_argument("-L", help="Looped Get run", action="store_true")
parser.add_argument("-l", "--logfile", help="File used so send looking informaion. DEFAULT =tokgen.log")
args = parser.parse_args()

# Arg Parse Main Stop ####

log_file = 'BatchGen.log'
log_level = 30
datafile = './Data/storelist2.xlsm'
chunk_size = args.c
outputfolder = '../Batches/'

group_file = args.i
pod = args.p
batchnum = args.b

# Init logging
init_logging()

try:
    # print(f'{datafile} {chunk_size} {outputfolder}' )
    xls = pandas.ExcelFile(datafile)
    df = xls.parse(xls.sheet_names[2]).set_index('NSN').T
    storedict = df.to_dict()
    newdict = {}
    # with open(datafile, 'r') as data:
    #     for line in csv.DictReader(data):
    #         while len(line['STORE_ID']) < 5:
    #             line['STORE_ID'] = '0' + line['STORE_ID']
    #         print (line['STORE_ID'])
    #
    # print(storedict[0])

    with open(group_file, 'r') as f:
        batch = f.readlines()
    initlen = len(batch)
    batchdict_list = []
    # # print(zipdb['TX'])
    num_reg = re.compile(r'_\d{5}')
    for store in batch:

        storenumsrch = num_reg.search(store)
        if storenumsrch:
            storenum = storenumsrch.group()
            storenum = storenum.replace('_', '')
            short_num = storenum_shorten(storenum)
        pri_vpn = storedict[short_num]['PRI_CONCENTRATOR_HOST_NAME'].split('.')
        pri_dc = pri_vpn[2][0:3].upper()
        # print(f'"{pri_dc}"')
        if pri_dc == 'RCS':
            dc_pref = 'DAL'
        elif pri_dc == 'SNA':
            dc_pref = 'SNA'
        else:
            print(f'[!] ERROR: No DC preference set for restaurant {short_num} defaulting to SNA')
            dc_pref = 'SNA'
        zone = pri_vpn = storedict[short_num]['AFFINITY_MEMBER_NAME']
        # print(f'{storenum},{pod},{dc_pref},{zone}')
        store_dict = {
            "site": "MCD_" + storenum,
            "Pod": pod,
            "DC": dc_pref,
            "zone": zone
        }
        batchdict_list.append(store_dict)
    if args.t:
        regions = {
            'central_southeast' : [],
            'west' : [],
            'north_east' : [],
            'midwest' : [],
            'south_central' : [],
            'southeast' : [],
        }
        for site in batchdict_list:
            # print(site['zone'])
            if 'midwest' in site['zone'].lower():
                regions['midwest'].append(site)
            elif 'west' in site['zone'].lower():
                regions['west'].append(site)
            elif 'central southeast' in site['zone'].lower():
                regions['central_southeast'].append(site)
            elif 'south-central' in site['zone'].lower():
                regions['south_central'].append(site)
            elif 'souththeast' in site['zone'].lower():
                regions['southeast'].append(site)
            elif 'north_east' in site['zone'].lower():
                regions['north_east'].append(site)
        for s,v in regions.items():
            if len(v) > 0:
                chunked = divide_chunks(v, chunk_size)
                filename = (f'{outputfolder}Batch_{batchnum}_{s}')
                filemake(filename, chunked)


        # print(central_southeast)
        # print(west)
        # print(north_east)
        # print(midwest)
        # print(south_central)
        # print(southeast)
        # if
    else:
    # print (json.dumps(batchdict_list,indent=2))
        chunked = divide_chunks(batchdict_list,chunk_size)
        # print(chunked)
        filename = (f'{outputfolder}Batch_{batchnum}_ALL')
        filemake(filename,chunked)






            # try:
    #             state = storedict["State"][short_num]
    #         except:
    #             print(f'Store number not found in store dict. removing from list')
    #             continue
    #         try:
    #             zonelong = (us.states.lookup(state).capital_tz)
    #         except:
    #             print(f'no state found for {store} Setting zone to westcoast')
    #             zonelong = 'America/Los_Angeles'
    #         zone = zonelong.strip('America/').replace("/", '_')
    #         stack = storedict["Stack"][short_num]
    #         store_dict = {'numberlong': storenum,
    #                       'numbershort': short_num,
    #                       'store_id': "MCD_" + storenum,
    #                       'group': store.strip(),
    #                       'state': state,
    #                       'zone': zone,
    #                       'stack': stack,
    #                       'pod': dc_matrix.color_coded[stack.lower()]['pod'],
    #                       'dc_priority': dc_matrix.color_coded[stack.lower()]['dc_priority']}
    #         batchdict_list.append(store_dict)
    #
    #     else:
    #         print(f'Store number find failed for {store}')
    #     timezones = {}
    #     for data in batchdict_list:
    #         try:
    #             timezones[data['zone']].append(data)
    #         except:
    #             timezones[data['zone']] = []
    #             timezones[data['zone']].append(data)
    #
    # for zone, data in timezones.items():
    #     # print (zone)
    #     chunked = divide_chunks(data, chunk_size)
    #     chunk_num = 1
    #     for chunk in chunked:
    #         with open('./tmp/chunk.json', 'w') as r:
    #             r.write(json.dumps(chunk, indent=2))
    #         # exit()
    #         chunk_name = zone + '_Batch_' + str(chunk_num)
    #         path = os.path.join('.\\', 'Batches\\')
    #         filename = path + chunk_name + '.xlsx'
    #         # print(filename)
    #         ck = pandas.DataFrame(data=chunk)
    #         ck.to_excel(filename, index=False, columns=['store_id', 'pod', 'dc_priority'])
    #         chunk_num += 1
except KeyboardInterrupt:
    print('Keyboard Interrupt received exiting script')
    exit()
