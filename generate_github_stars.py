#! /usr/bin/env python
import argparse
import urllib2
import json
import re
from generate_results import NumberOfUsers
from datetime import datetime

class GitHubStars (NumberOfUsers):

    def __init__(self, args):
        # Get args
        self.cached = args['cached']
        self.username = args['username']
        self.password = args['password']
        self.entries = []

        # Fetach from api
        url = 'https://api.github.com/repos/rethinkdb/rethinkdb/stargazers'
        if not self.cached:
            entries = self.fetch_from_api(url)
            keys = ["user", "starred_at"]
            def map_entries(d):
                return [d['user'], d['starred_at']]
            self.save_to_csv(map(map_entries, entries), keys, 'all-stars')
            self.save_to_json(map(map_entries, entries), keys, 'all-stars')
        else:
            self.entries = json.load(open('./results/results-all-stars.json'))



        # Show number of stars
        print 'Number of GitHub stars:', len(self.entries)

        per_month_count = self.get_per_month_count(self.entries)
        year_month_count = self.get_year_month_count(per_month_count)

        # Save
        keys = ["period", "count"]
        def map_entries(d):
            return [d["period"], d["count"]]
        self.save_to_csv(map(map_entries, year_month_count), keys, 'github-stars')
        self.save_to_json(map(map_entries, year_month_count), keys, 'github-stars')

    def get_per_month_count(self, entries):
        counts = {}
        for entry in entries:
            # 2012-10-30T05:37:47Z
            DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"
            file_date = datetime.strptime(entry['starred_at'], DATE_FMT)
            if file_date.year not in counts:
                counts[file_date.year] = {}
            if file_date.month not in counts[file_date.year]:
                counts[file_date.year][file_date.month] = 0
            counts[file_date.year][file_date.month] += 1
        return counts

    def get_year_month_count(self, per_month_count):
        counts = []
        for year, month_values in per_month_count.items():
            for month, value in month_values.items():
                counts.append({ "period": str(year) + '-' + str(month), "count": value })
        return counts

    def fetch_from_api(self, url):
        def encodeUserData(user, password):
            return "Basic " + (user + ":" + password).encode("base64").rstrip()
        req = urllib2.Request(url, headers = { 'Accept': 'application/vnd.github.star+json', 'Authorization': encodeUserData(self.username, self.password) })
        con = urllib2.urlopen(req)
        next_link = False
        if con.info()['link'] != "":
            links = con.info()['link'].split(',')
            for link in links:
                if link.find('rel="next"') > 0:
                    result = re.search(r"(<)([^<]+)(>)", link)
                    if len(result.groups()) == 3:
                        next_link = result.groups()[1]
                        print next_link
        response = json.loads(con.read())
        def map_users_time(entry):
            return {
                'user': entry['user']['login'],
                'starred_at': entry['starred_at']
            }
        entries = map(map_users_time, response)
        if next_link is not False:
            return entries + self.fetch_from_api(next_link)
        else:
            return entries

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get GitHub stars over time')
    parser.add_argument('--cached', action='store_true', help='use cached logs instead of fetching from GitHub API')
    parser.add_argument('--username', default=None, help='GitHub username')
    parser.add_argument('--password', default=None, help='GitHub password')
    args = vars(parser.parse_args())
    GitHubStars(args)


