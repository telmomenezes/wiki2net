#!/usr/bin/env python


__author__ = "Telmo Menezes (telmo@telmomenezes.com)"
__date__ = "Jul 2011"


"""
Copyright (C) 2011 Telmo Menezes.

This program is free software; you can redistribute it and/or modify
it under the terms of the version 2 of the GNU General Public License 
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""


import sys


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

    # import links from source
    cur_src.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM link")
    for row in cur_src:
        orig_id = row[0]
        targ_id = row[1]
        start_ts = row[2]
        end_ts = row[3]
        cur_src.execute("SELECT title, parsed FROM article WHERE id=?", (orig_id,))
        row2 = cur_src.fetchone()
        orig_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_src.execute("SELECT title, parsed FROM article WHERE id=?", (targ_id,))
        row2 = cur_src.fetchone()
        targ_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_dest.execute("INSERT INTO link (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, start_ts, end_ts))

    # import redirects from source
    cur_src.execute("SELECT orig_id, targ_id, start_ts, end_ts FROM redirect")
    for row in cur_src:
        orig_id = row[0]
        targ_id = row[1]
        start_ts = row[2]
        end_ts = row[3]
        cur_src.execute("SELECT title, parsed FROM article WHERE id=?", (orig_id,))
        row2 = cur_src.fetchone()
        orig_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_src.execute("SELECT title, parsed FROM article WHERE id=?", (targ_id,))
        row2 = cur_src.fetchone()
        targ_id = find_or_create_article(cur_dest, row2[0], row2[1])
        cur_dest.execute("INSERT INTO redirect (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, start_ts, end_ts))
    
    # commit, close connections and cursors
    conn_dest.commit()
    cur_dest.close()
    conn_dest.close()
    cur_src.close()
    conn_src.close()


def mergenets(dest, sources):
    for source in sources:
        merge(dest, source)


if __name__ == '__main__':
    mergenets(sys.argv[1], sys.argv[2:])
