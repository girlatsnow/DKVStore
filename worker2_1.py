import rpyc
import os
import sys
import subprocess
import logging
import socket
import random
logging.basicConfig(level=logging.INFO)

HOST = 'localhost'
PORT= 5050

class MyService(rpyc.Service):
    dictTable={}
    cnt = 0

    def exposed_createtable(self):

        self.cnt+=1
        self.dictTable[self.cnt]={}
        logging.info('new table '+str(self.cnt))
        return self.cnt

    def exposed_close(self):
        self.dictTable={}
        return 1


    def exposed_connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((HOST, PORT))
        logging.info('send connect to master')

    
    def exposed_set(self,i,a,b):
        logging.info('set talbe '+str(i)+': '+str(a)+'=>'+str(b))
        self.dictTable[i][a]=b
        return 1

    def exposed_update(self,i, a,b):
        logging.info('add table '+str(i)+': '+ str(a)+'+'+str(b))
        if a in self.dictTable[i]:
            self.dictTable[i][a]+=b
            return self.dictTable[i][a]
        else:
            logging.warning(str(a)+' is not in table '+str(i))
            return None

    def exposed_get(self,i,a):
        if a in self.dictTable[i]:
            logging.info('get '+str(i)+': '+str(a) + ' ' +str(self.dictTable[i][a]))
            return self.dictTable[i][a]
        else:
            logging.warning(str(a)+' is not in table '+str(i))
            return None

    def exposed_gettable(self, i):
        return self.dictTable[i]

    def exposed_pagerank(self, graph, curr, delta, factor):
        self.dictTable[delta]={}
        logging.info('pagerank on worker 1')
        for vid, adjs in self.dictTable[graph].iteritems():
            for adj in adjs:
                if not adj in self.dictTable[delta]:
                    self.dictTable[delta][adj]=0
                self.dictTable[delta][adj]+=factor* self.dictTable[curr][vid]*1.0/len(self.dictTable[graph][vid])
        logging.info('finish pagerank on worker 1')
        return self.dictTable[delta]

    def exposed_swaptable(self, frm, to):
        self.dictTable[to]=self.dictTable[frm]
        return 1
    def exposed_update_table2(self, frm, i, to, j):
        if not j in self.dictTable[to]:
            self.dictTable[to][j]=0
        self.dictTable[to][j]+=self.dictTable[frm][i]
        return 1

    def exposed_cleartable(self, i):
        self.dictTable[i]={}
        return 1

    def exposed_settable(self, i, table):
        logging.info('set table')
        self.dictTable[i]=table
        return 1

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer

    t = ThreadedServer( MyService, port = 18861,protocol_config={"allow_public_attrs":True})
    t.start()