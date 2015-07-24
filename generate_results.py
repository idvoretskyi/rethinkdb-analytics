#! /usr/bin/env python
import os, sys
import socket
import csv
import json
import calendar
from datetime import datetime, timedelta
import argparse
from collections import Counter
from prettytable import PrettyTable
from dateutil.relativedelta import relativedelta

# Roll a date back to the most recent Monday
def roll_back_a_week(adate):
    return adate - timedelta(days=adate.weekday())

# Roll a date forward a week
def roll_forward_a_week(adate):
    return adate + timedelta(7)

# Roll a date back to the first day of the month
def roll_back_a_month(adate):
    return adate - timedelta(days=(adate.day - 1))

# Roll a date forward a month
def roll_forward_a_month(adate):
    return adate + relativedelta(months=1)

# Pretty-print a date range
def pp_dates(from_date, to_date, date_fmt):
    #date_fmt = "%b %d"
    date_range = from_date.strftime(date_fmt) + " - " + to_date.strftime(date_fmt)
    return date_range

class NumberOfUsers():

    ROOT_DIR = 'update_logs'
    # This presumes that all your `.log` files are in the minor directory

    LOG_FILES_DIRS = {
        'minor': ['update_logs/minor'],
        'periodic': ['update_logs/periodic']
    }
    DATE_FMT = '%Y-%m-%d'
    SSH_LOGIN = 'teapot@rethinkdb.com'
    REMOTE_DIR = '/srv/www/update.rethinkdb.com/flask_logs/'

    def __init__(self, args):
        # Parse Args
        self.interval = args['interval']
        self.cached = args['cached']
        self.nohits = args['nohits']
        self.log_type = args['log_type']

        # Set Interval
        if self.interval not in ['week', 'month', 'geo']:
            print "%s is not a recognized interval, use: week, month; or geo" % self.interval
            sys.exit()

        # Fetch log files (and create the directory they'll live in if it doesn't exist)
        for directory in self.LOG_FILES_DIRS[self.log_type]:
            if not os.path.exists(directory):
                os.makedirs(directory)
        if not self.cached:
            os.system("rsync -Pavzrh -e 'ssh -p 440' %s:%s %s" % (self.SSH_LOGIN, self.REMOTE_DIR, self.ROOT_DIR))

        # Get IPs per table
        ips_per_date, all_uniques, first_date = self.get_ips_per_table(
            self.LOG_FILES_DIRS[self.log_type],
            self.DATE_FMT,
            self.log_type
        )

        # Get From/To date
        from_date, to_date = self.get_from_to_dates(self.interval, first_date, all_uniques)

        # Get Largest deployments
        if self.log_type == 'periodic':
            keys = ['ip', 'num_servers', 'num_tables', 'last_seen', 'first_seen', 'hits']
            most_servers, most_tables = self.get_10_largest_deployments(all_uniques, keys)
            def map_server_dict(d):
                return [d['ip'], d['num_servers'], d['num_tables'], d['last_seen'], d['first_seen'], d['hits']]
            self.save_to_csv(map(map_server_dict, most_servers), keys, 'most-servers')
            self.save_to_csv(map(map_server_dict, most_tables), keys, 'most-tables')
            self.save_to_json(map(map_server_dict, most_servers), keys, 'most-servers')
            self.save_to_json(map(map_server_dict, most_tables), keys, 'most-tables')

        # Get Buckets
        buckets  = self.get_buckets(ips_per_date, self.interval, from_date, to_date)

        # Build Table
        table_rows, existing_ips, all_ips = self.get_table(buckets)

        # Print Tables
        self.print_table(table_rows)
        self.print_ips(buckets, existing_ips, all_ips)
        self.print_ip_counts(all_ips)

        # Save to CSV
        keys = ["range", "uniques", "existing", "new"]
        self.save_to_csv(table_rows, keys, self.log_type + '-' + self.interval)
        self.save_to_json(table_rows, keys, self.log_type + '-' + self.interval)

    # Read all .log files and return all the IP tables in a particular time period
    # Each log files contains the date for a specific time period
    # It also returns a list of unique IP addresses with hits, first_seen, and last seen
    def get_ips_per_table(self, LOG_FILES_DIRS, DATE_FMT, log_type):
        ips_per_date = []
        all_uniques = {}
        for directory_name in LOG_FILES_DIRS:
            files = [f for f in os.listdir(directory_name) if str(f[-4:]) == ".log" and os.path.isfile(os.path.join(directory_name, f))]
            for f in sorted(files):
                file_date = datetime.strptime(os.path.splitext(f)[0], DATE_FMT)
                ips = []
                with open(os.path.join(directory_name,f), 'r') as log:
                    for row in log.readlines():
                        """
                        Minor:
                            timestamp, version, ip, user-agent, lang
                        Periodic:
                            timestamp, version, ip, num_servers, system, num_tables, ??
                        """
                        row = row.split('\t')
                        ip = row[2]
                        if log_type == 'periodic':
                            try:
                                num_servers = int(row[3])
                                num_tables = int(row[5])
                            except:
                                num_servers = 0
                                num_tables = 0
                        else:
                            num_servers = None
                            num_tables = None
                        ips.append(ip)
                        if ip in all_uniques:
                            ip_info = all_uniques[ip]
                            ip_info['hits'] += 1
                            ip_info['first_seen'] = min(ip_info['first_seen'], file_date)
                            ip_info['last_seen'] = max(ip_info['last_seen'], file_date)
                            if log_type == 'periodic':
                                ip_info['num_servers'] = max(ip_info['num_servers'], num_servers)
                                ip_info['num_tables'] = max(ip_info['num_tables'], num_tables)
                            all_uniques[ip] = ip_info
                        else:
                            ip_info = { 'ip': ip,
                                    'hits': 1,
                                    'first_seen': file_date,
                                    'last_seen': file_date,
                                    'num_servers': num_servers,
                                    'num_tables': num_tables }
                        all_uniques[ip] = ip_info
                ips_per_date.append({
                    'datetime': file_date,
                    'ips': ips,
                })
        first_date = ips_per_date[0]['datetime']
        return ips_per_date, all_uniques, first_date

    def get_host(self, ip):
        try:
            return (ip, socket.gethostbyaddr(ip)[0])
        except:
            return (ip, "")

    def get_from_to_dates (self, interval, first_date, all_uniques):
        if interval == 'week':
            from_date = roll_back_a_week(first_date)
            to_date = roll_forward_a_week(from_date)
            return from_date, to_date

        if interval == 'month':
            from_date = roll_back_a_month(first_date)
            to_date = roll_forward_a_month(from_date)
            return from_date, to_date

        if interval == 'geo':
            print "Starting reverse ip lookup. This will take some time..."
            print "Note: there may be duplicate entries for different IPs that resolve to the same host"
            # generate count,host pairs
            from multiprocessing import Pool
            import pretty
            pool = Pool(256)
            hosts = pool.map(self.get_host, all_uniques)
            rows = []
            for (ip, host) in hosts:
                if host != "":
                    hostname = host
                else:
                    hostname = ip
                rows.append((all_uniques[ip]['hits'],
                            hostname,
                            pretty.date(all_uniques[ip]['first_seen']),
                            pretty.date(all_uniques[ip]['last_seen'])))
            # sort by count
            rows.sort()
            # prettify it
            x = PrettyTable(["hits", "host", "first seen", "last seen"])
            for row in rows:
                x.add_row(row)
            print x
            sys.exit(0)

    def get_buckets(self, ips_per_date, interval, from_date, to_date):
        i = 0
        buckets = []
        bucket = []
        while True:
            # We've gone through all the log files
            if i >= len(ips_per_date):
                # If this bucket had any dates in it, keep the partial bucket, then end the loop
                if len(bucket) > 0:
                    buckets.append(bucket)
                break

            date = ips_per_date[i]['datetime']
            # If the date of this log file is out of range, this bucket is full
            if not from_date <= date < to_date:
                buckets.append(bucket)
                bucket = []

                # Move the range up to the next interval (week / month)
                from_date = to_date
                if interval == 'week':
                    to_date = roll_forward_a_week(to_date)
                elif interval == 'month':
                    to_date = roll_forward_a_month(to_date)
                continue

            # Keep building this bucket!
            bucket.append(ips_per_date[i])
            i += 1
        return buckets

    def get_table(self, buckets):
        existing_ips = set() # this is a set so each element only appears exactly once
        all_ips = []
        table_rows = [] # rows for the results table
        for bucket in buckets:
            ips_for_bucket = [ip for day in bucket for ip in day['ips']] # list comprehension for nested lists
            all_ips += ips_for_bucket # first add to the global set

            # set operations to determine vital stats
            uniques_for_bucket = set(ips_for_bucket)
            new_for_bucket = uniques_for_bucket - existing_ips
            num_existing = len(uniques_for_bucket) - len(new_for_bucket)

            # add onto our list of known ips
            existing_ips.update(uniques_for_bucket)

            # record a summary for this row
            daterange = pp_dates(bucket[0]['datetime'], bucket[-1]['datetime'], self.DATE_FMT)
            table_rows.append([daterange, len(uniques_for_bucket), num_existing, len(new_for_bucket), len(ips_for_bucket)])
        return table_rows, existing_ips, all_ips

    def get_10_largest_deployments(self, all_unique_ips, keys):
        num_servers = sorted(all_unique_ips.values(), key=lambda k: k['num_servers'], reverse=True)[:25]
        num_tables= sorted(all_unique_ips.values(), key=lambda k: k['num_tables'], reverse=True)[:25]

        def filter_keys(orig_dict):
            return dict(zip(keys, [orig_dict[k] for k in keys]))
        num_servers = map(filter_keys, num_servers)
        num_tables = map(filter_keys, num_tables)
        return num_servers, num_tables

    def print_table(self, table_rows):
        # Build the table
        ptable_array = ["range","uniques","existing","new"]
        if not self.nohits:
            ptable_array.append("hits")
        x = PrettyTable(ptable_array)
        for row in table_rows:
            x.add_row(row[:-1] if self.nohits else row)
        print x

    def print_ips(self, buckets, existing_ips, all_ips):
        daterange = pp_dates(buckets[0][0]['datetime'], buckets[-1][-1]['datetime'], self.DATE_FMT)
        print "Total stats for %s:" % daterange
        print "\t%d uniques" % len(existing_ips)
        if not self.nohits:
            print "\t%d hits" % len(all_ips)

    def print_ip_counts (self, all_ips):
        counter = Counter(all_ips)
        dcounter = dict(counter)
        counts = {}
        for i in range(1, 8): counts[i] = 0
        for key in dcounter:
            for i in range(1, 8):
                if dcounter[key] >= i: counts[i] += 1
        print 'Counts:', counts

    def save_to_csv(self, table_rows, headers, name):
        if not os.path.exists('results'):
            os.makedirs('results')
        with open('results/results-' + name + '.csv', 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            writer.writerow(headers)
            for row in table_rows:
                writer.writerow(row)

    def save_to_json(self, table_rows, headers, name):
        def json_serialize(obj):
            """Default JSON serializer."""
            if isinstance(obj, datetime):
                if obj.utcoffset() is not None:
                    obj = obj - obj.utcoffset()
            millis = int(
                calendar.timegm(obj.timetuple()) * 1000 +
                obj.microsecond / 1000
            )
            return millis
        if not os.path.exists('results'):
            os.makedirs('results')
        with open('results/results-' + name + '.json', 'wb') as f:
            def map_server_array(d):
                mapped_dict = {}
                for i, key in enumerate(headers):
                    mapped_dict[key] = d[i]
                return mapped_dict
            f.write(json.dumps(map(map_server_array, table_rows), default=json_serialize))
            f.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Report usage statistics based on an interval. Accepts an interval in days, and aggregates data over this interval since the beginning of time.')
    parser.add_argument('interval', default='week', help='interval to analyze: `week`, `month`; alternatively, specify `geo` to do reverse ip lookup')
    parser.add_argument('--log_type', default='both', help='Directory to scan. Options: `both`, minor`, `periodic`')
    parser.add_argument('--cached', action='store_true', help='use cached logs instead of fetching new logs via rsync.')
    parser.add_argument('--nohits', action='store_true', help='do not print the hits as part of the results.')
    args = vars(parser.parse_args())
    if args['log_type'] == 'both':
        NumberOfUsers(dict(args, **{ 'log_type': 'minor'}))
        NumberOfUsers(dict(args, **{ 'log_type': 'periodic'}))
    else:
        NumberOfUsers(args)

