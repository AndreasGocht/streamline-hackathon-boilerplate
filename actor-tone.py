#!/usr/bin/python

import re
import sys
import operator

tonesum = {}
tonecount = {}

finaltonesum = 0
finaltonecount = 0

for line in open(sys.argv[1]):
    items = line.split('\t')
    actor = items[5]
    avgtone = items[34]
    tonesum[actor] = tonesum.get(actor,0)+ float(avgtone)
    tonecount[actor] = tonecount.get(actor, 0) + 1
    finaltonesum += float(avgtone)
    finaltonecount += 1

print "overall avgtone: %f" % (finaltonesum /finaltonecount) 

for key, value in sorted(tonecount.items(),key=operator.itemgetter(1), reverse=True):
    if tonecount[key] > 20:
        print "%s\t%f\t%d" % (key, tonesum[key] / tonecount[key], tonecount[key])

