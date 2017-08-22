import argparse
import os
import os.path
from matplotlib import pyplot as pp
import time
import re

parser = argparse.ArgumentParser(description='shows events.')
parser.add_argument('-p','--path', help = "path that holds the data")

args = parser.parse_args()


f,sb = pp.subplots(3,1, sharex = True)

news_paper = []

while True:
    
    tot_avg_tone_gl = {}
    avg_tone_gl = {}
    error_gl = {}
    index = 0
    indexes = {}

    for p in sorted(os.walk(args.path)):
        if "_SUCCESS" in p[2]:
            tot_avg_tone = {}
            avg_tone = {}
            error = {}
            index = index + 1

            for filename in p[2]:
                if re.match("part-[0-9]*$",filename):
                    filepath = os.path.join(p[0],filename)
                    
                    with open(filepath) as f:
                        for line in f:
                            line = line.strip("\n")
                            dat = line.split(",")
                            #print(dat[0],dat[1])
                            #print("---")
                            
                            tot_avg_tone.setdefault((dat[0]),[]).append(float(dat[2]))
                            avg_tone.setdefault((dat[0]),[]).append(float(dat[3]))
                            error.setdefault((dat[0]),[]).append(float(dat[4]))
                            if dat[1] not in news_paper:
                                news_paper.append(dat[1])

            for country,vals in tot_avg_tone.items():
                tot_avg_tone_gl.setdefault((country),[]).append(sum(vals)/len(vals))
                indexes.setdefault((country),[]).append(index)
               
            for country,vals in avg_tone.items():
                avg_tone_gl.setdefault((country),[]).append(sum(vals)/len(vals))
               
            for country,vals in error.items():
                sum_tmp = 0
                for elem in vals:
                    sum_tmp = sum_tmp + abs(elem)
                error_gl.setdefault((country),[]).append(sum_tmp/len(vals))
                    
    print("------") 
                                
    
    sb[0].cla()
    for key,val in tot_avg_tone_gl.items():
        sb[0].plot(indexes[key], val,"-x",label = key)
    sb[0].axes.get_xaxis().set_visible(False)
    
    sb[1].cla()
    for key,val in avg_tone_gl.items():
        sb[1].plot(indexes[key], val,"-x",label = key)
    sb[1].axes.get_xaxis().set_visible(False)

    sb[2].cla()
    for key,val in error_gl.items():
        sb[2].plot(indexes[key], val,"-x",label = key)
    sb[2].axes.get_xaxis().set_visible(False)


    sb[0].legend(loc='upper left')
    sb[1].legend(loc='upper left')
    pp.pause(2)
    
