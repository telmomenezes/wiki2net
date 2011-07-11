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
import sqlite3
from xml.etree import ElementTree as ET
import re


def safe_execute(cur, query):
    try:
        cur.execute(query)
    except sqlite3.OperationalError:
        pass


def create_db(dbpath):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    # create articles table
    safe_execute(cur, "CREATE TABLE articles (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN wos_id TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN title TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN abstract TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN issue_id INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN type TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN beginning_page INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN end_page INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN page_count INTEGER")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN language TEXT")
    safe_execute(cur, "ALTER TABLE articles ADD COLUMN timestamp REAL")
    
    # create citations table
    safe_execute(cur, "CREATE TABLE citations (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN orig_id INTEGER")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN targ_id INTEGER")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN orig_wosid TEXT")
    safe_execute(cur, "ALTER TABLE citations ADD COLUMN targ_wosid TEXT")
    
    # indexes
    safe_execute(cur, "CREATE INDEX articles_id ON articles (id)")
    safe_execute(cur, "CREATE INDEX articles_wos_id ON articles (wos_id)")
    safe_execute(cur, "CREATE INDEX issues_id ON issues (id)")
    safe_execute(cur, "CREATE INDEX issues_wos_id ON issues (wos_id)")
    safe_execute(cur, "CREATE INDEX publications_ISSN ON publications (ISSN)")
    safe_execute(cur, "CREATE INDEX authors_name ON authors (name)")
    safe_execute(cur, "CREATE INDEX keywords_keyword ON keywords (keyword)")
    safe_execute(cur, "CREATE INDEX organizations_name ON organizations (name)")
    safe_execute(cur, "CREATE INDEX author_citations_id ON author_citations (id)")
    safe_execute(cur, "CREATE INDEX author_citations_orig_targ ON author_citations (orig_id, targ_id)")
    safe_execute(cur, "CREATE INDEX article_author_article_id ON article_author (article_id)")

    conn.commit()
    cur.close()
    conn.close()


def wiki2net(source, dbpath):
    count = 0
    for event, elem in ET.iterparse(source, events=('start', 'end')):
        if event == 'end':
            tag = elem.tag
            if tag.find('title') >= 0:
                print count, '==== TITLE: ', elem.text, ' ===='
                count += 1
            elif tag.find('text') >= 0:
                if elem.text is not None:
                    matches = re.findall('\[\[([^\]]*)\]\]', elem.text)
                    for m in matches:
                        target = m.split('|')[0]
                        target = target.split('#')[0]
                        print target
            elem.clear()


if __name__ == '__main__':
    wiki2net(sys.argv[1], sys.argv[2])
