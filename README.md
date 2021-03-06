# wiki2net

Wiki2net is a set of scripts used to extract a dynamic network from Wikipedia database backup dumps. The nodes of the extracted network are articles and the edges are citations and redirections. We only consider citations that are internal to Wikipedia - one article linking to another article. Links have a begin and end timestamp associated with them, so it is trivial to extract a snapshot of the Wikipedia article network at a certain point in time.

The network is stored on a SQLite3 database file, so that it is easy to manipulate with other tools. The scripts are written in Python.

Wikipedia database dumps can be obtained here: http://dumps.wikimedia.org.

## Simple usage

The main script is wiki2net.py. It reads a Wikipedia dump file from the stdin and writes the extracted network to the sqlite3 database file passed as the first parameter. If the file does not exit, it is created. A simple usage example would be:

    bzcat enwiki-20110405-pages-meta-history1.xml.bz2 | ./wiki2net.py wikipedia.db

So the idea here is to take advantage of the bzcat UNIX command to avoid having to decompress the dump files to disk. The simplest way to extract the network is to sequentially process all the dump files to the same sqlite3 file.

## Database schema

The database schema that was defined to contain the network is the following:

    CREATE TABLE article (id INTEGER PRIMARY KEY, title TEXT, parsed INTEGER DEFAULT 0);
    CREATE TABLE link (id INTEGER PRIMARY KEY, orig_id INTEGER, targ_id INTEGER, start_ts INTEGER, end_ts INTEGER);
    CREATE TABLE redirect (id INTEGER PRIMARY KEY, orig_id INTEGER, targ_id INTEGER, start_ts INTEGER, end_ts INTEGER);
    CREATE INDEX article_id ON article (id);
    CREATE INDEX article_title ON article (title);

## Parallel processing

Processing files takes a considerable amount of time. The author's core i7-2600 machine is taking aprox. one day per dump file. One obvious way to speed things up is to process several dump files at the same time. You can process each file to a separate database file and then merge all the outputs with the merge.py script:

    ./merge.py target.db src1.db src2.db ...

## Technical details

Wikipedia dump files are very large xml files that include the content of every revision for every article. Wiki2net has to parse this content to find out when citations and redirections are created or removed.

## Author

Wiki2net is being developed by Telmo Menezes (telmo@telmomenezes.com). Feel free to contact the author with an issues regarding this software.

## License

Wiki2net is released under the GPLv2 open source public license. The full text of the license can be found on the COPYING file.

