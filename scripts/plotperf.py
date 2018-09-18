#! /usr/bin/env python3
#! -*- coding:utf-8 -*-

# Author: bugnofree
# Filename: plotperf.py
# Date: 2018-09-18
# Contact: pwnkeeper@gmail.com

import struct
import sys
import matplotlib.pyplot as plt
from matplotlib import collections  as mc
import numpy as np
import os

def extract_perfvals(perffile):
    perf = dict()
    chunkseq = 0
    fperf = open(perffile, 'rb')
    while 1:
        tmbin = fperf.read(16)
        if not tmbin: break
        chunkseq += 1
        start, stop = struct.unpack("qq", tmbin)
        perf[chunkseq] = {'start': start, 'stop': stop}
    starttm = [perf[tm]['start'] for tm in perf.keys()]
    basetm = sorted(starttm)[0]
    for key in perf.keys():
        perf[key]['start'] -= basetm
        perf[key]['stop'] -= basetm
    fperf.close()
    return perf

def plotperf(perfdir, namelist, title, outname):
    """ Plot performance graph

    :param perfdir: A string. The directory of the performance data.
    :param namelist: A list of integers. The performance data name, looks like '8', '12' ...
    :param title: The figure title.
    :param outname: The output figure name.

    """
    fig, axes = plt.subplots(len(namelist), 2, sharey = 'row', sharex = True)

    colormap = plt.cm.get_cmap('brg')

    for name, rowid in zip(namelist, range(len(namelist))):
        fmt = "{name:s}MB.iso.{direction:s}.{chunksz:d}.perf"
        fperfrecv = fmt.format(name = str(name), direction = "send", chunksz = 4194304)
        fperfsend = fmt.format(name = str(name), direction = "recv", chunksz = 4194304)

        perfsend = extract_perfvals(os.path.join(perfdir, fperfsend))
        perfrecv = extract_perfvals(os.path.join(perfdir, fperfrecv))

        sendlines = [[(perfsend[key]['start'], key), (perfsend[key]['stop'], key)] for key in perfsend.keys()]
        recvlines = [[(perfrecv[key]['start'], key), (perfrecv[key]['stop'], key)] for key in perfrecv.keys()]
        colors = [colormap(i) for i in np.linspace(0, 1, len(sendlines))]

        rowax = axes[rowid]
        for axid, lines in zip(range(len(rowax)), [sendlines, recvlines]):
            rowax[axid].add_collection(mc.LineCollection(lines, colors = colors, linewidths = 2))
            rowax[axid].autoscale()
            if axid == 0:
                rowax[axid].set_ylabel("Chunk ID(%s MB)" % (namelist[rowid]))
            gap = 8
            if len(sendlines) < gap:
                stride = 1
            else:
                stride = len(sendlines) // gap
            rowax[axid].set_yticks(range(1, len(sendlines) + 1, stride))

        if rowid == 0:
            rowax[0].set_title("Send Chunk")
            rowax[1].set_title("Recv Chunk")
        if rowid == len(namelist) - 1:
            rowax[0].set_xlabel("Time(ms)")
            rowax[1].set_xlabel("Time(ms)")

    plt.suptitle(title)
    #  plt.subplots_adjust(wspace = 0.2, hspace = 0.3)
    #  plt.tight_layout()
    #  plt.savefig("{:s}.svg".format(outname))
    #  plt.savefig("{:s}.png".format(outname))
    plt.show()

if __name__ == '__main__':
    pass
    #  onenodedir = ""
    #  four_nodes_dir = ""
    #  plotperf(onenodedir, [8, 64, 128, 256], "One Node Testing", "onenode")
    #  plotperf(four_nodes_dir, [8, 64, 128, 256], "Four Nodes Testing", "fournodes")
