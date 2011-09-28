#!/usr/bin/env python


__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Sep 2011"


import sys
import sqlite3


def rmredirects(dbpath):

    print 'Starting redirect removal process.'

    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur2 = conn.cursor()

    count = 0

    cur.execute("SELECT count(id) FROM redirect")
    total = cur.fetchone()[0]

    cur.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM redirect")
    for row in cur:
        orig_id = row[0]
        targ_id = row[1]
        start_ts = row[2]
        end_ts = row[3]
        cur2.execute("UPDATE link SET orig_id=? WHERE orig_id=? AND start_ts>=? AND end_ts<=?", (targ_id, orig_id, start_ts, end_ts))   
        cur2.execute("UPDATE link SET targ_id=? WHERE targ_id=? AND start_ts>=? AND end_ts<=?", (targ_id, orig_id, start_ts, end_ts))
        count += 1
        if (count % 1000) == 0:
            print '%f%%' % ((float(count) / float(total)) * 100.0) 
 
    # final commit and close db cursor and connection
    conn.commit()
    cur.close()
    cur2.close()
    conn.close()

    print 'Done.'


if __name__ == '__main__':
    rmredirects(sys.argv[1])
