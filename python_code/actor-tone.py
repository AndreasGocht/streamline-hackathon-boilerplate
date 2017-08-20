#!/usr/bin/python3

import re
import sys
import operator

tonesum = {}
tonecount = {}

finaltonesum = 0
finaltonecount = 0

path = sys.argv[1]
finaltonecount_max = int(sys.argv[2])


with open(path) as f:
    for line in f:
        items = line.split('\t')
        actor = items[5]
        avgtone = items[34]
        url = items[-1]
        m = re.match(r'[^/]*://([^/]*)/',url)
        domain = "unkonwn"
        if m:
            domain = m.group(1)
 
        finaltonesum += float(avgtone)
        finaltonecount += 1
        if (finaltonecount > finaltonecount_max):
            break

        #if "yahoo" not in domain:
            #continue
        if actor is not "":
            continue
            
        tonesum[(domain,actor)] = tonesum.get((domain,actor),0)+ float(avgtone)
        tonecount[(domain,actor)] = tonecount.get((domain,actor), 0) + 1

print ("overall avgtone: %f" % (finaltonesum /finaltonecount) )


for key, value in sorted(tonecount.items(),key=operator.itemgetter(1), reverse=True):
    if tonecount[key] > 20:
        print ("%s\t%f\t%d" % (key, tonesum[key] / tonecount[key], tonecount[key]))

