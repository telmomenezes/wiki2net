#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Oct 2011"


import sys
import sqlite3
from pymongo import Connection


def net2mongo(dbpath, mongodb):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    mconn = Connection()
    mdb = mconn[mongodb]
    mnodes = mdb.nodes
    medges = mdb.edges
  
    cur.execute("SELECT count(id) FROM article")
    ncount = cur.fetchone()[0]
    print ncount, 'nodes total'

    cur.execute("SELECT count(id) FROM link")
    lcount = cur.fetchone()[0]
    print lcount, 'links total'

    count = 0
    nodes = {}
    cur.execute("SELECT id, title FROM article")
    for row in cur:
        label = '%s [%d]' % (row[1], row[0])
        nodes[row[0]] = mnodes.insert({'label':label})
        if (count % 100000) == 0:
            print 'adding nodes %f%% (%d/%d)' % ((float(count)/ float(ncount)) * 100, count, ncount)
        count += 1

    count = 0
    cur.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM link")
    for row in cur:
        medges.insert({'orig': nodes[row[0]], 'targ': nodes[row[1]], 'start_ts': row[2], 'end_ts': row[3]})
        if (count % 100000) == 0:
            print 'adding links %f%% (%d/%d)' % ((float(count)/ float(lcount)) * 100, count, lcount)
        count += 1

    cur.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    net2mongo(sys.argv[1], sys.argv[2])
