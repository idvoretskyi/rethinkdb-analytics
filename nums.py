#! /usr/bin/env python
"""
# accepts interval in days, and aggregates data over
# this interval since beginning of time                                                                                                                                                                  
$ usage_data.py 7
#   hits   uniques   actives
----------------------------
1   500    100       40
2   700    140       50
3   900    180       70
...
"""
import os, sys
from datetime import datetime, timedelta
import argparse
from prettytable import PrettyTable

LOG_FILES_DIR = 'update_logs'
DATE_FMT = '%Y-%m-%d'
MIN_ACTIVE = 3

# Function to return formatted stats based on a date range
def get_stats(from_date, to_date):
    data_set = []
    found_log_file = False
    for f in sorted(os.listdir(LOG_FILES_DIR)):
        file_date = datetime.strptime(os.path.splitext(f)[0], DATE_FMT)

        if from_date < file_date <= to_date:
            found_log_file = True
            with open(os.path.join(LOG_FILES_DIR,f),'r') as log:
                for row in log.readlines():
                    data_point = row.split()
                    new_point = {
                        'datetime': data_point[0]+data_point[1],
                        'version': data_point[2],
                        'ip': data_point[3],
                        'useragent': ''.join(str(elem) for elem in data_point[4:])
                    }
                    data_set.append(new_point)

    # uniques
    unique_data_set = {v['ip']: v for v in data_set}.values()

    # actives
    num_times_per_ip = {}
    for data_point in data_set:
        ip = data_point['ip']
        if ip in num_times_per_ip:
            num_times_per_ip[ip] += 1
        else:
            num_times_per_ip[ip] = 1

    active_data_set = filter(lambda x: x > MIN_ACTIVE, num_times_per_ip.values())

    return (found_log_file,data_set,unique_data_set,active_data_set)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Report usage statistics based on an interval. Accepts an interval in days, and aggregates data over this interval since the beginning of time.')
parser.add_argument('interval', nargs=1, default=7, help='Interval to analyze in days')
parser.add_argument('--cached', action='store_true')
args = parser.parse_args()

# Fetch log files (and create the directory they'll live in if it doesn't exist)
if not os.path.exists(LOG_FILES_DIR):
    os.makedirs(LOG_FILES_DIR)
if not args.cached:
    os.system("rsync -Pavzrh -e 'ssh -p 440' teapot@rethinkdb.com:/srv/www/update.rethinkdb.com/flask_logs/ %s" % LOG_FILES_DIR)

to_date = datetime.today()
from_date = to_date - timedelta(int(args.interval[0]))

x = PrettyTable(["#","hits","uniques","actives"])
i = 1
while True:
    found_log_file, data_set, unique_data_set, active_data_set = get_stats(from_date, to_date)
    if not found_log_file:
        break
    x.add_row([i, len(data_set), len(unique_data_set), len(active_data_set)])
    i += 1
    to_date = from_date
    from_date = to_date - timedelta(int(args.interval[0]))

print x
