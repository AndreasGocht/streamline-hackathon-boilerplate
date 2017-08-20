import argparse
import os
from matplotlib import pyplot as pp
import time

parser = argparse.ArgumentParser(description='shows events.')
parser.add_argument('-p','--path', help = "path that holds the data")

args = parser.parse_args()

fig = pp.figure()
pp.show()

while True:
    data = {}

    for p in sorted(os.listdir(args.path)):
        print(p)
        filename = args.path + "/" + p+"/part-00001"
        while not os.path.isfile(filename):
            time.sleep(0.1)
        with open(filename) as f:
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
    
    
    
    
    
    
    
