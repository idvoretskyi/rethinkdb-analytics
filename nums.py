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

Stats = collections.namedtuple('Stats','found_log_file, data_set, unique_data_set, active_data_set')

# Function to return formatted stats based on a date range
def get_stats(from_date, to_date):
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

    return Stats(found_log_file=found_log_file, data_set=data_set, unique_data_set=unique_data_set, active_data_set=active_data_set)

# Generate a row that can be pretty-printed, based on the output of get_stats
def pretty_print_stats(stats, from_date, to_date):
    date_fmt = "%b %d"
    date_range = from_date.strftime(date_fmt) + " - " + to_date.strftime(date_fmt)
    return [date_range, len(stats.data_set), len(stats.unique_data_set), len(stats.active_data_set)]

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
    table_rows.append(pretty_print_stats(stats,from_date, to_date))

    # Roll the date back
    to_date = from_date - timedelta(1)
    if interval == 'week':
        from_date = roll_back_a_week(to_date)
    elif interval == 'month':
        from_date = roll_back_a_month(to_date)

# Calculate stats for the last row (across all log files)
sum_table_row = pretty_print_stats(get_stats(from_date, today), from_date, today)

x = PrettyTable(["date range","hits","uniques","actives"])
# Print out the rows of the table in reverse order
for row in table_rows[::-1]:
    x.add_row(row)
print x

y = PrettyTable(["totals (all dates)", "total hits", "total uniques", "total actives"])
y.add_row(sum_table_row)
print y

