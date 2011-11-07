#!/usr/bin/python2.6

# Copyright 2011 Cloudera Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Script for creating a Gephi GEXF file from a CSV of drug-drug-reaction triples."""

__author__ = "jwills@cloudera.com (Josh Wills)"

import csv
import sys

drugs = []
drug_ids = {}
rels = {}
for row in csv.reader(open(sys.argv[1]), delimiter='$'):
  d1, d2 = row[0], row[1]
  if "/" not in d1 and "/" not in d2:
    for d in (d1, d2):
      if d not in drug_ids:
        drug_ids[d] = len(drugs)
        drugs.append(d)

    id1, id2 = drug_ids[d1], drug_ids[d2]
    if id1 not in rels:
      rels[id1] = {}
    if id2 not in rels[id1]:
      rels[id1][id2] = []
    rels[id1][id2].append((row[2], int(row[3]), float(row[7])))

print """<?xml version="1.0" encoding="UTF8"?>
<gexf xmlns="http://www.gexf.net/1.2draft"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd"
 version="1.2">
  <meta lastmodifieddate="20111012">
    <creator>Cloudera</creator>
    <description>A network of drug-drug relationships</description>
  </meta>
  <graph defaultedgetype="undirected">
    <attributes class="edge">
      <attribute id="0" title="reactions" type="string"/>
    </attributes>
    <nodes>"""

for i, drug in enumerate(drugs):
  print "      <node id=\"%d\" label=\"%s\"/>" % (i, drug)

print "    </nodes>"
print "    <edges>"

edge_id = 0
for id1, id_rel in rels.iteritems():
  for id2, values in id_rel.iteritems():
    weight = max(x[2] for x in values)
    reactions = "$".join(x[0] for x in values)  
    print ("      <edge id=\"%d\" source=\"%d\" target=\"%d\" weight=\"%.2f\">" %
        (edge_id, id1, id2, weight))
    print "        <attvalues><attvalue for=\"0\" value=\"%s\"/></attvalues>" % reactions
    print "      </edge>"
    edge_id += 1
print "    </edges>"
print "  </graph>"
print "</gexf>"
