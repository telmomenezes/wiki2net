# wiki2net

Wiki2net is a set of scripts used to extract a dynamic network from Wikipedia database backup dumps. The nodes of the extracted network are articles and the edges are citations and redirections. We only consider citations that are internal to Wikipedia - one article linking to another article. Links have a begin and end timestamp associated with them, so it is trivial to extract a snapshot of the Wikipedia article network at a certain point in time.

The network is stored on a SQLite3 database file, so that it is easy to manipulate with other tools. The scripts are written in Python.

Wikipedia database dumps can be obtained here: http://dumps.wikimedia.org.

## Simple usage

The main script is wiki2net.py. It reads a Wikipedia dump file from the stdin and writes the extracted network to the sqlite3 database file passed as the first parameter. If the file does not exit, it is created. A simple usage example would be:

    bzcat enwiki-20110405-pages-meta-history1.xml.bz2 | ./wiki2net.py wikipedia.db

## Author

Wiki2net is being developed by Telmo Menezes (telmo@telmomenezes.com). Feel free to contact the author with an issues regarding this software.

## License

Wiki2net is released under the GPLv2 open source public license. The full text of the license can be found on the COPYING file.

