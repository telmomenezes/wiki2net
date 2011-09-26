#!/usr/bin/env python


__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jul 2011"


import sys
import sqlite3


def find_or_create_article(cur, title, parsed):
    cur.execute("SELECT id, parsed FROM article WHERE title=?", (title,))
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO article (title, parsed) VALUES (?, ?)", (title, parsed))
        return cur.lastrowid
    else:
        article_id = row[0]
        if (row[1] == 0) and (parsed == 1):
            cur.execute("UPDATE article SET parsed=1 WHERE id=?", (article_id,))
        return article_id


def merge(dest, source):
    conn_dest = sqlite3.connect(dest)
    cur_dest = conn_dest.cursor()
    conn_src = sqlite3.connect(source)
    cur_src = conn_src.cursor()
    cur_src2 = conn_src.cursor()

    # import links from source
    cur_src.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM link")
    for row in cur_src:
        orig_id = row[0]
        targ_id = row[1]
        start_ts = row[2]
        end_ts = row[3]
        cur_src2.execute("SELECT title, parsed FROM article WHERE id=?", (orig_id,))
        row2 = cur_src2.fetchone()
        orig_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_src2.execute("SELECT title, parsed FROM article WHERE id=?", (targ_id,))
        row2 = cur_src2.fetchone()
        targ_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_dest.execute("INSERT INTO link (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, start_ts, end_ts))

    # import redirects from source
    cur_src.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM redirect")
    for row in cur_src:
        orig_id = row[0]
        targ_id = row[1]
        start_ts = row[2]
        end_ts = row[3]
        cur_src2.execute("SELECT title, parsed FROM article WHERE id=?", (orig_id,))
        row2 = cur_src2.fetchone()
        orig_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_src2.execute("SELECT title, parsed FROM article WHERE id=?", (targ_id,))
        row2 = cur_src2.fetchone()
        targ_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_dest.execute("INSERT INTO redirect (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, start_ts, end_ts))
    
    # commit, close connections and cursors
    conn_dest.commit()
    cur_dest.close()
    conn_dest.close()
    cur_src.close()
    cur_src2.close()
    conn_src.close()


if __name__ == '__main__':
    merge(sys.argv[1], sys.argv[2])
