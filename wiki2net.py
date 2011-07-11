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


NAMESPACES = ('User', 'Wikipedia', 'File', 'MediaWiki', 'Template', 'Help', 'Category', 'Thread', 'Summary', 'Portal', 'Book', 'Special', 'Media', 'Talk', 'User talk', 'Wikipedia talk', 'File talk', 'MediaWiki talk', 'Template talk', 'Help talk', 'Category talk', 'Thread talk', 'Summary talk', 'Portal talk', 'Book talk', 'WP', 'Project', 'WT', 'Project Talk', 'Image', 'Image Talk', 'MOS', 'CAT', 'P', 'T', 'H', 'C', 'MP', 'wiktionary', 'wikt', 'wikinews', 'n', 'wikibooks', 'b', 'wikiquote', 'q', 'wikisource', 's', 'oldwikisource', 'species', 'wikispecies', 'wikiversity', 'v', 'wikimedia', 'foundation', 'wmf', 'commons', 'meta', 'metawikipedia', 'm', 'strategy', 'incubator', 'mw', 'quality', 'bugzilla', 'mediazilla', 'nost', 'testwiki', 'wmar', 'wmau', 'wmca', 'wmcz', 'wmde', 'wmfi', 'wmhk', 'wmhu', 'wmin', 'wmid', 'wmil', 'wmit', 'wmnl', 'wmno', 'wmpl', 'wmru', 'wmrs', 'wmse', 'wmch', 'wmtw', 'wmuk', 'en', 'de', 'fr', 'it', 'pl', 'es', 'ja', 'nl', 'ru', 'pt', 'sv', 'zh', 'ca', 'no', 'uk', 'fi', 'vi', 'cs', 'hu', 'tr', 'id', 'ko', 'ro', 'fa', 'da', 'ar', 'eo', 'sr', 'lt', 'sk', 'he', 'ms', 'vo', 'bg', 'sl', 'eu', 'war', 'hr', 'hi', 'et', 'az', 'gl', 'simple', 'nn', 'new', 'th', 'el', 'roa-rup', 'la', 'ht', 'tl', 'ka', 'kk', 'te', 'mk', 'sh', 'nap', 'ceb', 'pms', 'br', 'be-x-old', 'lv', 'jv', 'mr', 'ta', 'sq', 'lb', 'cy', 'is', 'bs', 'be', 'oc', 'yo', 'an', 'bpy', 'bn', 'io', 'sw', 'lmo', 'fy', 'gu', 'mg', 'ml', 'af', 'nds', 'ur', 'scn', 'pnb', 'qu', 'ku', 'zh-yue', 'ne', 'ast', 'su', 'hy', 'ga', 'bat-smg', 'cv', 'wa', 'am', 'kn', 'tt', 'diq', 'als', 'tg', 'vec', 'roa-tara', 'zh-min-nan', 'yi', 'bug', 'gd', 'os', 'uz', 'sah', 'pam', 'arz', 'mi', 'hsb', 'sco', 'li', 'nah', 'mn', 'my', 'co', 'gan', 'glk', 'ia', 'hif', 'bcl', 'fo', 'sa', 'si', 'fiu-vro', 'nds-nl', 'bar', 'mrj', 'vls', 'tk', 'ckb', 'gv', 'ilo', 'se', 'map-bms', 'dv', 'nrm', 'pag', 'rm', 'mzn', 'bo', 'udm', 'fur', 'wuu', 'ug', 'ps', 'mt', 'csb', 'lij', 'km', 'pi', 'bh', 'ang', 'koi', 'kv', 'lad', 'sc', 'nov', 'zh-classical', 'mhr', 'cbk-zam', 'ksh', 'kw', 'rue', 'frp', 'so', 'hak', 'nv', 'pa', 'szl', 'xal', 'ie', 'rw', 'stq', 'haw', 'pdc', 'ln', 'ext', 'krc', 'to', 'pcd', 'ky', 'crh', 'ace', 'myv', 'eml', 'gn', 'ba', 'ce', 'arc', 'kl', 'or', 'ay', 'pap', 'frr', 'bjn', 'pfl', 'jbo', 'wo', 'tpi', 'kab', 'ty', 'srn', 'gag', 'zea', 'dsb', 'lo', 'ab', 'ig', 'mdf', 'tet', 'av', 'kg', 'mwl', 'lbe', 'rmy', 'na', 'kaa', 'ltg', 'cu', 'kbd', 'as', 'sm', 'mo', 'bm', 'ik', 'bi', 'sd', 'ss', 'ks', 'iu', 'pih', 'pnt', 'cdo', 'chr', 'got', 'ee', 'ha', 'za', 'ti', 'bxr', 'om', 'zu', 've', 'ts', 'rn', 'sg', 'dz', 'tum', 'cr', 'ch', 'lg', 'fj', 'ny', 'st', 'xh', 'ff', 'tn', 'ki', 'sn', 'chy', 'ak', 'tw', 'ng', 'ii', 'cho', 'mh', 'aa', 'kj', 'ho', 'mus', 'kr', 'hz', 'nan')


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


def check_namespace(title, ns):
    l = len(ns) + 1
    return title[:l] == (ns + ':')


def main_namespace(title):
    # if title does not contain a colon, it can only belong to the main namespace
    if ':' not in title:
        return True

    # otherwise we have to check for every namespace to be sure
    for ns in NAMESPACES:
        if check_namespace(title, ns):
            return False

    return True


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
                        if main_namespace(target):
                            print target
            elem.clear()


if __name__ == '__main__':
    wiki2net(sys.argv[1], sys.argv[2])
