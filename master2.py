__author__ = 'licui'
__email__ = 'cui.judy.lee@gmail.com'
# created on 2015/7/3 11:17
#coding: utf-8
import rpyc
import threading
import time
import socket
import logging
import random
from rpyc.utils.server import ThreadedServer
addr = [('localhost',18861), ('localhost',18862)]
logging.basicConfig(level=logging.INFO)

class master(rpyc.Service):
    socks = []

    def exposed_init(self):
        self.alive=True
        for i in range(len(addr)):
            t = threading.Thread(target = self.run, args=(addr[i][0],addr[i][1],))
            t.setDaemon(True)
            t.start()

        while len(self.socks)<len(addr):
            pass


    def run(self, host, port):
        #connect to worker
        sock = rpyc.connect(host, port, config={"allow_all_attrs": True})
        self.socks.append(sock)


    def exposed_get(self,i,a):
        res =  self.socks[a%len(addr)].root.get(i,a)
        logging.info('get %d', res)
        return res

    def exposed_set(self,i, a, b):
        return self.socks[a%len(addr)].root.set(i,a, b)

    def exposed_update(self,i, k, delta):
        return self.socks[k%len(addr)].root.update(i, k, delta)

    def exposed_update_table2(self, frm, i, to, j):
        return self.socks[i%len(addr)].root.update_table2(frm, i, to, j)

    def exposed_create_table(self):
        for s in self.socks:
            i = s.root.createtable()
        return i

    def exposed_cleartable(self, i):
        cnt=0
        for s in self.socks:
            cnt +=  s.root.cleartable(i)
        return cnt

    def exposed_swaptable(self, frm, to):
        cnt = 0
        for s in self.socks:
            cnt +=  s.root.swaptable(frm, to)
        return cnt

    def exposed_pagerank(self, graph, curr, next, factor):
        self.pr = 0
        self.resnext = {}

        # collect next -> res
        logging.info('pagerank')
        for s in self.socks:
            t = threading.Thread(target = self.t_pr, args=(s, graph, curr, next,factor,))
            t.start()
        while self.pr<len(addr):
            pass

        # swap res -> curr
        logging.info('swap table')
        self.exposed_settable(curr, self.resnext)

        return 1

    def t_pr(self, s, graph, curr, next, factor):
        logging.info('thread pr')
        tmp = s.root.pagerank(graph, curr, next, factor)
        for vid, rank in  tmp.iteritems():
                if vid in self.resnext:
                    self.resnext[vid]+=rank
                else:
                    self.resnext[vid]=rank
        self.pr+=1
        logging.info('thread pr finish')

    def t_settable(self, s, i, curr, table):
        logging.info('thread set table')
        tmp={}
        for x, r in table.iteritems():
            if x%len(addr)==i:
                tmp[x]=r
        why = s.root.settable(curr, tmp)
        self.st += why
        logging.info('thread set table finish')

    def exposed_settable(self, to, table):
        self.st = 0
        for i,s in enumerate(self.socks):
            t = threading.Thread(target = self.t_settable, args=(s, i, to, table))
            t.start()
        while self.st<len(addr):
            pass
        return 1


    def exposed_initpr(self, graphdata, gtable, currtable):
        self.exposed_settable(gtable, graphdata)
        currdata = {}
        for k in graphdata.iterkeys():
            currdata[k]=random.random()
        self.exposed_settable( currtable, currdata)
        return 1

    def exposed_gettable(self, i):
        res = {}
        for s in self.socks:
            t = s.root.gettable(i)
            res.update(t)
        return res
if __name__ == "__main__":

    masterforclient = ThreadedServer( master, port = 5050,protocol_config={"allow_all_attrs":True})
    masterforclient.start()




    while(True):
        pass



