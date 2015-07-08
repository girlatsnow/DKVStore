__author__ = 'licui'
__email__ = 'cui.judy.lee@gmail.com'
# created on 2015/7/7 13:36
#coding: utf-8

import rpyc
import random

class client:
    def __init__(self, host='localhost', port=5050):
        self.sock = rpyc.connect(host, port, config={"allow_all_attrs": True})
        self.sock.root.init()
    def get(self,i,a):
        return self.sock.root.get(i,a)
    def set(self,i, a, b):
        return self.sock.root.set(i, a, b)
    def update(self,i, k, delta):
        return self.sock.root.update(i,k,delta)
    def create_table(self):
        return self.sock.root.create_table()
    def update_table2(self, frm, i, to, j):
        return self.sock.root.update_table2( frm, i, to, j)
    def swaptable(self, frm, to):
        return self.sock.root.swaptable(frm, to)
    def cleartable(self, i):
        return self.sock.root.cleartable(i)
    def pagerank(self, graph, curr, next, factor):
        return self.sock.root.pagerank(graph, curr, next, factor)
    def initpr(self, graphdata, gtable, ctable):
        return self.sock.root.initpr(graphdata, gtable, ctable)
    def get_table(self, i):
        return self.sock.root.gettable(i)


if __name__== '__main__':
    m = client()

    curr = m.create_table()
    next = m.create_table()

    graph = {}
    graph[1]=[2,3]
    graph[2]=[3]
    graph[3]=[1]

    g = m.create_table()

    m.initpr(graph, g, curr)

    print m.get_table(curr)

    m.pagerank(g, curr, next, 0.85)

    print m.get_table(curr)

    m.cleartable(curr)
    m.cleartable(g)
    m.cleartable(next)



