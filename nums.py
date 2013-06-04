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
import collections
from prettytable import PrettyTable

LOG_FILES_DIR = 'update_logs'
DATE_FMT = '%Y-%m-%d'
MIN_ACTIVE = 3

Stats = collections.namedtuple('Stats','found_log_file, data_set, unique_data_set, active_data_set, num_new, num_existing, new_ips_set')

# Function to return formatted stats based on a date range
def get_stats(from_date, to_date, existing_ips=[]):
    data_set = []
    found_log_file = False
    for f in sorted(os.listdir(LOG_FILES_DIR)):
        file_date = datetime.strptime(os.path.splitext(f)[0], DATE_FMT)

        if from_date <= file_date <= to_date:
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

    # figure out how many of our users are new or existing
    # start with the set of ips in this data set
    ips = [v['ip'] for v in data_set]
    ips = list(set(ips)) # wholly inefficient but quick way to get a unique set of values
    new_ips = list(set(ips) - set(existing_ips)) # new ips
    num_new = len(new_ips)
    num_existing = len(ips) - len(new_ips)

    return Stats(found_log_file=found_log_file, data_set=data_set, unique_data_set=unique_data_set, active_data_set=active_data_set, num_new=num_new, num_existing=num_existing, new_ips_set = new_ips)

# Roll a date back to the most recent Monday
def roll_back_a_week(adate):
    return adate - timedelta(days=adate.weekday())

# Roll a date back to the first day of the month
def roll_back_a_month(adate):
    return adate - timedelta(days=(adate.day - 1))

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Report usage statistics based on an interval. Accepts an interval in days, and aggregates data over this interval since the beginning of time.')
parser.add_argument('interval', nargs=1, default='week', help='interval to analyze: week, month')
parser.add_argument('--cached', action='store_true', help='use cached logs instead of fetching new logs via rsync.')
args = parser.parse_args()

interval = args.interval[0]
if interval != 'week' and interval != 'month':
    print "%s is not a recognized interval, use: week, month" % interval
    sys.exit()

# Fetch log files (and create the directory they'll live in if it doesn't exist)
if not os.path.exists(LOG_FILES_DIR):
    os.makedirs(LOG_FILES_DIR)
if not args.cached:
    os.system("rsync -Pavzrh -e 'ssh -p 440' teapot@rethinkdb.com:/srv/www/update.rethinkdb.com/flask_logs/ %s" % LOG_FILES_DIR)

# Get the list of log files we have, and sort by date
log_dates = []
for f in sorted(os.listdir(LOG_FILES_DIR)):
    file_date = datetime.strptime(os.path.splitext(f)[0], DATE_FMT)
    log_dates.append(file_date)

today = datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
to_date = today
if interval == 'week':
    from_date = roll_back_a_week(to_date)
elif interval == 'month':
    from_date = roll_back_a_month(to_date)

# We go through dates in reverse (start from today, going back until there are no more log
# entries), so we build the rows of the table and then later print them out in reverse
table_rows = []
while True:
    stats = get_stats(from_date, to_date)
    if not stats.found_log_file:
        break

    # pretty print dates
    date_fmt = "%b %d"
    date_range = from_date.strftime(date_fmt) + " - " + to_date.strftime(date_fmt)

    table_rows.append([date_range, len(stats.data_set), len(stats.unique_data_set), stats.num_new, stats.num_existing])

    # Roll the date back
    to_date = from_date - timedelta(1)
    if interval == 'week':
        from_date = roll_back_a_week(to_date)
    elif interval == 'month':
        from_date = roll_back_a_month(to_date)

x = PrettyTable(["date range","hits","uniques","new users", "existing users"])
# Print out the rows of the table in reverse order
for row in table_rows[::-1]:
    x.add_row(row)
print x

# Calculate stats for the last row (across all log files)
total_stats = get_stats(from_date, today)

# pretty print dates
date_fmt = "%b %d"
date_range = from_date.strftime(date_fmt) + " - " + to_date.strftime(date_fmt)

print "Total stats for %s:\n\t%d hits\n\t%d unique users" % (date_range, len(total_stats.data_set), len(total_stats.unique_data_set))

