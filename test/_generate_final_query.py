#!/usr/bin/env python3

from __future__ import print_function
import codecs
import os
import random
import stat
import sys
import time


def random_date(start, end, rand, format="%Y-%m-%d"):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.

    See: https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + rand.random() * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


if __name__ == '__main__':
    seed = int(sys.argv[1])
    print("seed: %d" % seed)
    rand = random.Random(seed)

    query_count = rand.randint(800, 1000)  # at least 800 queries
    print("query_count: %d" % query_count)

    file_path = "./final_seed_%d_query_%d" % (seed, query_count)
    fp = codecs.open(file_path, "wb", encoding="utf8")
    fp.write('#!/bin/bash\n')
    fp.write('\n')
    fp.write('set -e\n')
    fp.write('\n')
    fp.write('cd "$(dirname "$0")"\n')
    fp.write('\n')
    fp.write('time taskset -c 0-15 ../run.sh \\\n')
    fp.write('    ../data/customer.txt ../data/orders.txt ../data/lineitem.txt \\\n')
    fp.write('    %d \\\n' % query_count)
    for _ in range(query_count):
        mktsegment = rand.choice(["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"])
        orderdate = random_date("1991-10-01", "1999-02-01", rand)
        shipdate = random_date("1991-10-01", orderdate, rand)
        limit = rand.randint(80000, 100000)  # at least top 80000
        fp.write("    %-10s %s %s %d \\\n" % (mktsegment, orderdate, shipdate, limit))

    fp.write('    \\\n')
    fp.write('    >"$(basename "$0").stdout"\n')
    fp.write('\n')
    fp.write('if [ -z "$BDCI19_SKIP_DIFF" ]; then\n')
    fp.write('    echo\n')
    fp.write('    diff "$(basename "$0").stdout" "$(basename "$0").result"\n')
    fp.write('fi\n')
    fp.write('\n')
    fp.close()

    st = os.stat(file_path)
    os.chmod(file_path, st.st_mode | stat.S_IEXEC)

