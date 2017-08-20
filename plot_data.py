import argparse
import os
import os.path
from matplotlib import pyplot as pp
import time
import re

parser = argparse.ArgumentParser(description='shows events.')
parser.add_argument('-p','--path', help = "path that holds the data")

args = parser.parse_args()

fig = pp.figure()


while True:
    data = {}

    for p in sorted(os.walk(args.path)):
        if "_SUCCESS" in p[2]:
            for filename in p[2]:
                if re.match("part-[0-9]*$",filename):
                    filepath = os.path.join(p[0],filename)
                    with open(filepath) as f:
                        for line in f:
                            line = line.strip("\n")
                            dat = line.split(",")
                            print(dat[0])
                            print("---")
                            print(data.get(dat[0],[]))
                            if dat[0] in data:
                                print("hi")
                            data.setdefault(dat[0],[]).append(float(dat[1]))
                
    fig.clf()
    for key,val in data.items():
        pp.plot(val,"-x",label = key)
    pp.legend(loc='upper left')
    pp.pause(5)
    
    
    
    
    
    
    
