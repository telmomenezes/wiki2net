#!/usr/bin/env python
# encoding: utf-8

__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Oct 2011"


import sys
import sqlite3
from syn.net import Net


def net2syn(dbpath, outpath):
    net = Net(outpath)

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
  
    cur.execute("SELECT count(id) FROM article")
    ncount = cur.fetchone()[0]
    print ncount, 'nodes total'

    cur.execute("SELECT count(id) FROM link")
    ncount = cur.fetchone()[0]
    print lcount, 'links total'

    count = 0
    nodes = {}
    cur.execute("SELECT id, title, FROM article")
    for row in cur:
        label = '%s [%d]' % (row[1], row[0])
        nodes[row[0]] = net.add_node(label=label)
        if (count % 1000) == 0:
            print '%f%% (%d/%d)' % ((float(count)/ float(ncount)), count, ncount)
        count += 1

    count = 0
    cur.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM link")
    for row in cur:
        net.add_edge(nodes[row[0]], nodes[row[1]], row[2], row[3])
        if (count % 1000) == 0:
            print '%f%% (%d/%d)' % ((float(count)/ float(lcount)), count, lcount)
        count += 1

    cur.close()
    conn.close()

    print('Done.')


if __name__ == '__main__':
    net2syn(sys.argv[1], sys.argv[2])
