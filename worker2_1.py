import rpyc
import os
import sys
import subprocess
import logging
import socket
import random
from enum import enum
import pickle
import threading
import time
from rpyc.utils.server import ThreadedServer


logging.basicConfig(level=logging.INFO)
logging.FileHandler('worker1.log')

HOST = 'localhost'
PORT= 5051

DIR = 'checkpoint_18861'


status = 0


class MyService(rpyc.Service):
    dictTable={}
    cnt = 0
    working = False

    def on_connect(self):
        print 'on connect'
        logging.info('on connect')
        self.status = status
        if not self.status== 0:
            print 'load checkpoint'
            for fname in os.listdir(DIR):
                #if not fname=='graph':
                    f = open(os.path.join(DIR, fname), 'rb')
                    mp = pickle.load(f)
                    f.close()
                    self.dictTable[int(fname)] = mp
            #
            # gf = open(os.path.join(DIR, 'graph'), 'r')
            # gid = int(gf.readline().strip())
            # self.dictTable[gid]={}
            # for line in gf:
            #     gl = line.split()
            #     self.dictTable[gid][int(gl[0])]=map(int, gl[1:])

        self.working = True

        print 'working status: ', self.working

    def exposed_createtable(self):

        while not self.working:
            pass

        self.cnt+=1
        self.dictTable[self.cnt]={}
        logging.info('new table '+str(self.cnt))
        return self.cnt

    def exposed_close(self):
        while not self.working:
            pass
        self.dictTable={}
        return 1


    def exposed_connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((HOST, PORT))
        logging.info('send connect to master')

    
    def exposed_set(self,i,a,b):
        while not self.working:
            pass
        logging.info('set talbe '+str(i)+': '+str(a)+'=>'+str(b))
        self.dictTable[i][a]=b
        return 1

    def exposed_update(self,i, a,b):
        while not self.working:
            pass
        logging.info('add table '+str(i)+': '+ str(a)+'+'+str(b))
        if a in self.dictTable[i]:
            self.dictTable[i][a]+=b
            return self.dictTable[i][a]
        else:
            logging.warning(str(a)+' is not in table '+str(i))
            return None

    def exposed_get(self,i,a):
        while not self.working:
            pass
        if a in self.dictTable[i]:
            logging.info('get '+str(i)+': '+str(a) + ' ' +str(self.dictTable[i][a]))
            return self.dictTable[i][a]
        else:
            logging.warning(str(a)+' is not in table '+str(i))
            return None

    def exposed_gettable(self, i):
        while not self.working:
            pass
        return self.dictTable[i]

    def clearnext(self, next, curr):
        if not next in self.dictTable:
            self.dictTable[next]={}
        for k in self.dictTable[curr]:
            self.dictTable[next][k]=0

    def exposed_pagerank(self, graph, curr, delta, factor):

        while not self.working:
            pass

        self.clearnext(delta, curr)
        logging.info('pagerank on worker 1')
        cnt = 0
        print 'pagerank'
        for vid, adjs in self.dictTable[graph].iteritems():
            cadjs = len(adjs)
            for adj in adjs:
                if adj not in self.dictTable[delta]:
                    self.dictTable[delta][adj]=0
                self.dictTable[delta][adj]+=factor* self.dictTable[curr][vid]*1.0/cadjs
        logging.info('finish pagerank on worker 1')
        print 'finish pagerank'
        return self.dictTable[delta]

    def exposed_swaptable(self, frm, to):
        while not self.working:
            pass
        self.dictTable[to]=self.dictTable[frm]
        return 1
    def exposed_update_table2(self, frm, i, to, j):
        while not self.working:
            pass
        if not j in self.dictTable[to]:
            self.dictTable[to][j]=0
        self.dictTable[to][j]+=self.dictTable[frm][i]
        return 1

    def exposed_cleartable(self, i):
        while not self.working:
            pass
        self.dictTable[i]={}
        return 1

    def exposed_settable(self, i, table):
        while not self.working:
            pass
        logging.info('set table')
        self.dictTable[i].update(table)

        return 1

    def exposed_initpr(self, graph, curr, next):
        print 'init pr'
        while not self.working:
            pass
        print 'initpr'
        self.working=0

        gtable ={}
        gtable.update(self.dictTable[graph])
        self.dictTable[graph]={}
        nedge = 0
        for v in gtable:
            self.dictTable[graph][v]=[]
            nedge+=len(gtable[v])
            for adj in gtable[v]:
                self.dictTable[graph][v].append(adj)
        print 'nVertex:', len(gtable), 'nEdge:', nedge

        for k in self.dictTable[graph].iterkeys():
            self.dictTable[curr][k]=random.random()
            self.dictTable[next][k]=0
        print 'number of vertices: ', len(self.dictTable[graph])
        if os.path.exists(DIR):
            for f in os.listdir(DIR):
                os.remove(os.path.join(DIR, f))
        else:
            os.mkdir(DIR)
        self.working=1

        print 'checkpoint curr'
        self.exposed_restore(curr)
        print 'checkpoint graph'
        self.exposed_restore(graph)
        print 'init finish'
        return 1



    def exposed_restore(self, tableid):
        while not self.working:
            pass

        f=open(os.path.join(DIR,str(tableid)), 'wb')
        #print tableid, f.name, self.dictTable[tableid]
        pickle.dump(self.dictTable[tableid], f, pickle.HIGHEST_PROTOCOL)
        #print 0
        f.close()



def runserver(Sv, prt):
    masterforworker = ThreadedServer( Sv, port = prt,protocol_config={"allow_all_attrs":True, "allow_pickle":True}, listener_timeout=20000)
    masterforworker.start()

if __name__ == "__main__":
    mastersock = rpyc.connect(HOST, PORT, config={"allow_all_attrs": True, "allow_pickle":True})
    status = mastersock.root.getworkerstatus('localhost',18861)
    workermanager = threading.Thread(target = runserver, args=(MyService, 18861, ))
    workermanager.start()
    time.sleep(2)
    mastersock.root.connectwk('localhost', 18861)



