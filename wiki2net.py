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


from settings import *
import sys
import sqlite3
from xml.etree import ElementTree as ET
import re
import time


NAMESPACES = ('User', 'Wikipedia', 'File', 'MediaWiki', 'Template', 'Help', 'Category', 'Thread', 'Summary', 'Portal', 'Book', 'Special', 'Media', 'Talk', 'talk', 'User talk', 'Wikipedia talk', 'File talk', 'MediaWiki talk', 'Template talk', 'Help talk', 'Category talk', 'Thread talk', 'Summary talk', 'Portal talk', 'Book talk', 'WP', 'Project', 'WT', 'Project Talk', 'Image', 'Image Talk', 'MOS', 'CAT', 'P', 'T', 'H', 'C', 'MP', 'wiktionary', 'wikt', 'wikinews', 'n', 'wikibooks', 'b', 'wikiquote', 'q', 'wikisource', 's', 'oldwikisource', 'species', 'wikispecies', 'wikiversity', 'v', 'wikimedia', 'foundation', 'wmf', 'commons', 'meta', 'metawikipedia', 'm', 'strategy', 'incubator', 'mw', 'quality', 'bugzilla', 'mediazilla', 'nost', 'testwiki', 'wmar', 'wmau', 'wmca', 'wmcz', 'wmde', 'wmfi', 'wmhk', 'wmhu', 'wmin', 'wmid', 'wmil', 'wmit', 'wmnl', 'wmno', 'wmpl', 'wmru', 'wmrs', 'wmse', 'wmch', 'wmtw', 'wmuk', 'en', 'de', 'fr', 'it', 'pl', 'es', 'ja', 'nl', 'ru', 'pt', 'sv', 'zh', 'ca', 'no', 'uk', 'fi', 'vi', 'cs', 'hu', 'tr', 'id', 'ko', 'ro', 'fa', 'da', 'ar', 'eo', 'sr', 'lt', 'sk', 'he', 'ms', 'vo', 'bg', 'sl', 'eu', 'war', 'hr', 'hi', 'et', 'az', 'gl', 'simple', 'nn', 'new', 'th', 'el', 'roa-rup', 'la', 'ht', 'tl', 'ka', 'kk', 'te', 'mk', 'sh', 'nap', 'ceb', 'pms', 'br', 'be-x-old', 'lv', 'jv', 'mr', 'ta', 'sq', 'lb', 'cy', 'is', 'bs', 'be', 'oc', 'yo', 'an', 'bpy', 'bn', 'io', 'sw', 'lmo', 'fy', 'gu', 'mg', 'ml', 'af', 'nds', 'ur', 'scn', 'pnb', 'qu', 'ku', 'zh-yue', 'ne', 'ast', 'su', 'hy', 'ga', 'bat-smg', 'cv', 'wa', 'am', 'kn', 'tt', 'diq', 'als', 'tg', 'vec', 'roa-tara', 'zh-min-nan', 'yi', 'bug', 'gd', 'os', 'uz', 'sah', 'pam', 'arz', 'mi', 'hsb', 'sco', 'li', 'nah', 'mn', 'my', 'co', 'gan', 'glk', 'ia', 'hif', 'bcl', 'fo', 'sa', 'si', 'fiu-vro', 'nds-nl', 'bar', 'mrj', 'vls', 'tk', 'ckb', 'gv', 'ilo', 'se', 'map-bms', 'dv', 'nrm', 'pag', 'rm', 'mzn', 'bo', 'udm', 'fur', 'wuu', 'ug', 'ps', 'mt', 'csb', 'lij', 'km', 'pi', 'bh', 'ang', 'koi', 'kv', 'lad', 'sc', 'nov', 'zh-classical', 'mhr', 'cbk-zam', 'ksh', 'kw', 'rue', 'frp', 'so', 'hak', 'nv', 'pa', 'szl', 'xal', 'ie', 'rw', 'stq', 'haw', 'pdc', 'ln', 'ext', 'krc', 'to', 'pcd', 'ky', 'crh', 'ace', 'myv', 'eml', 'gn', 'ba', 'ce', 'arc', 'kl', 'or', 'ay', 'pap', 'frr', 'bjn', 'pfl', 'jbo', 'wo', 'tpi', 'kab', 'ty', 'srn', 'gag', 'zea', 'dsb', 'lo', 'ab', 'ig', 'mdf', 'tet', 'av', 'kg', 'mwl', 'lbe', 'rmy', 'na', 'kaa', 'ltg', 'cu', 'kbd', 'as', 'sm', 'mo', 'bm', 'ik', 'bi', 'sd', 'ss', 'ks', 'iu', 'pih', 'pnt', 'cdo', 'chr', 'got', 'ee', 'ha', 'za', 'ti', 'bxr', 'om', 'zu', 've', 'ts', 'rn', 'sg', 'dz', 'tum', 'cr', 'ch', 'lg', 'fj', 'ny', 'st', 'xh', 'ff', 'tn', 'ki', 'sn', 'chy', 'ak', 'tw', 'ng', 'ii', 'cho', 'mh', 'aa', 'kj', 'ho', 'mus', 'kr', 'hz', 'nan')


STATE_OUT = 0
STATE_INPAGE = 1
STATE_INREVISION = 2


def safe_execute(cur, query):
    try:
        cur.execute(query)
    except sqlite3.OperationalError:
        pass


def create_db(dbpath):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()

    # create article table
    safe_execute(cur, "CREATE TABLE article (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE article ADD COLUMN title TEXT")
    
    # create link table
    safe_execute(cur, "CREATE TABLE link (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE link ADD COLUMN orig_id INTEGER")
    safe_execute(cur, "ALTER TABLE link ADD COLUMN targ_id INTEGER")
    safe_execute(cur, "ALTER TABLE link ADD COLUMN start_ts INTEGER")
    safe_execute(cur, "ALTER TABLE link ADD COLUMN end_ts INTEGER")

    # create redirect table
    safe_execute(cur, "CREATE TABLE redirect (id INTEGER PRIMARY KEY)")
    safe_execute(cur, "ALTER TABLE redirect ADD COLUMN orig_id INTEGER")
    safe_execute(cur, "ALTER TABLE redirect ADD COLUMN targ_id INTEGER")
    safe_execute(cur, "ALTER TABLE redirect ADD COLUMN start_ts INTEGER")
    safe_execute(cur, "ALTER TABLE redirect ADD COLUMN end_ts INTEGER")
    
    # indexes
    safe_execute(cur, "CREATE INDEX article_id ON article (id)")
    safe_execute(cur, "CREATE INDEX article_title ON article (title)")

    conn.commit()
    cur.close()
    conn.close()


def main_namespace(title):
    # if title does not contain a colon, it can only belong to the main namespace
    if ':' not in title:
        return True

    # otherwise we have to check for every namespace to be sure
    if title.split(':')[0] in NAMESPACES:
        return False

    return True


def process_links(revision_links, open_links, page_links, ts):
    for l in revision_links:
        if l in open_links:
            if open_links[l][1] >= 0:
                ts0 = open_links[l][0]
                ts1 = open_links[l][1]
                if (ts - ts1) > STABILITY:
                    if (ts1 - ts0) > STABILITY:
                        page_links.append((l, ts0, ts1))
                    open_links[l][0] = ts
                    open_links[l][1] = -1
                else:
                    open_links[l][1] = ts
        else:
            open_links[l] = [ts, -1]

    for l in open_links:
        if open_links[l][1] < 0:
            if l in revision_links:
                open_links[l][1] = ts


def process_links_final(open_links, page_links):
    for l in open_links:
        ts0 = open_links[l][0]
        ts1 = open_links[l][1]
        if (ts1 < 0) or ((ts1 - ts0) > STABILITY):
            page_links.append((l, ts0, ts1))


def processed_redirs(redirs):
    # remove redirs that are too short lived
    new_list = []
    for r in redirs:
        if (r[2] < 0) or ((r[2] - r[1]) > STABILITY):
            new_list.append(r)

    return new_list
 

def find_or_create_article(cur, title):
    cur.execute("SELECT id FROM article WHERE title=?", (title,))
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO article (title) VALUES (?)", (title,))
        return cur.lastrowid
    else:
        return row[0]


def write2db(cur, page_title, links, page_redirs):
    orig_id = find_or_create_article(cur, page_title)

    for l in links:
        targ_id = find_or_create_article(cur, l[0])
        cur.execute("INSERT INTO link (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, l[1], l[2]))

    for r in page_redirs:
        targ_id = find_or_create_article(cur, r[0])
        cur.execute("INSERT INTO redirect (orig_id, targ_id, start_ts, end_ts) VALUES (?, ?, ?, ?)", (orig_id, targ_id, r[1], r[2]))

    
def normalize_title(title):
    stitle = title.strip(' \t\n\r')
    norm_title = ''
    if len(stitle) > 0:
        norm_title = stitle[0].upper()
    if len(stitle) > 1:
        norm_title += stitle[1:]

    if '_' in norm_title:
        norm_title = norm_title.replace('_', ' ')

    return norm_title


def parse_link_markup(markup):
    target = markup.split('|')[0]
    target = target.split('#')[0]
    if len(target) > 0:
        if main_namespace(target):
            target = normalize_title(target)
            if len(target) > 0:
                return target

    return None


def wiki2net(dbpath):
    create_db(dbpath)
    
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    
    page_title = ''
    revision_links = []
    open_links = {}
    page_links = []
    page_redirs = []
    cur_redir = ''
    cur_redir_ts = -1
    new_redir = ''
    revision_ts = ''
    state = STATE_OUT

    count = 0
    for event, elem in ET.iterparse(sys.stdin, events=('start', 'end')):
        tag = elem.tag

        if event == 'start':
            if state == STATE_OUT:
                if tag.find('page') >= 0:
                    state = STATE_INPAGE
                    open_links = {}
                    page_links = []
                    page_redirs = []
                    cur_redir = ''
                    cur_redir_ts = -1

            elif state == STATE_INPAGE:
                if tag.find('revision') >= 0:
                    state = STATE_INREVISION
                    revision_links = []
                    new_redir = ''
        
        if event == 'end':
            if state == STATE_INPAGE:
                if tag.find('page') >= 0:
                    process_links_final(open_links, page_links)
                    if cur_redir != '':
                        page_redirs.append((cur_redir, cur_redir_ts, -1))
                    page_redirs = processed_redirs(page_redirs)

                    write2db(cur, page_title, page_links, page_redirs)
                    conn.commit()
                    
                    count += 1
                    print 'Article #%d: %s; links: %d; redirects: %d' % (count, page_title, len(page_links), len(page_redirs))
                    
                    state = STATE_OUT

                elif tag.find('title') >= 0:
                    # only process articles from the main namespace
                    if main_namespace(elem.text):
                        page_title = normalize_title(elem.text)
                    else:
                        state = STATE_OUT

            elif state == STATE_INREVISION:
                if tag.find('revision') >= 0:
                    process_links(revision_links, open_links, page_links, revision_ts)
                    if cur_redir != new_redir:
                        if cur_redir != '':
                            page_redirs.append((cur_redir, cur_redir_ts, revision_ts))
                        cur_redir = new_redir
                        if cur_redir != '':
                            cur_redir_ts = revision_ts
                        else:
                            cur_redir_ts = -1
                    state = STATE_INPAGE

                elif tag.find('timestamp') >= 0:
                    revision_ts = int(time.mktime(time.strptime(elem.text, '%Y-%m-%dT%H:%M:%SZ')))

                elif tag.find('text') >= 0:
                    if elem.text is not None:
                        matches = re.findall('\[\[([^\]]*)\]\]', elem.text)

                        if elem.text[:9] == '#REDIRECT':
                            if len(matches) > 0:
                                target = parse_link_markup(matches[0])
                                if target is not None:
                                    new_redir = target
                        else:
                            new_redir = ''
                            for m in matches:
                                target = parse_link_markup(m)
                                if target is not None:
                                    revision_links.append(target)
            
            # clear current element to limit memory usage
            elem.clear()

    # final commit and close db cursor and connection
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    wiki2net(sys.argv[1])
