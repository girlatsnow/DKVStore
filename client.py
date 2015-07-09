__author__ = 'licui'
__email__ = 'cui.judy.lee@gmail.com'
# created on 2015/7/7 13:36
#coding: utf-8

import rpyc

MASTERHOST = 'localhost'
MASTERPORT = '5050'


class client:
    def __init__(self, host='localhost', port=5050):
        self.sock = rpyc.connect(host, port, config={"allow_all_attrs": True, "allow_pickle":True}, keepalive=True)

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
        apr = rpyc.async(self.sock.root.pagerank)
        res = apr(graph, curr, next, factor)
        while not res.ready:
            pass
        return res.value
    def initpr(self, graphdata, gtable, ctable, ntable):
        ai = rpyc.async(self.sock.root.initpr)
        res = ai(graphdata, gtable, ctable, ntable)
        while not res.ready:
            pass
        print 'init finish'
        return res.value
    def get_table(self, i):
        agt = rpyc.async(self.sock.root.gettable)
        res = agt(i)
        while not res.ready:
            pass
        return res.value


if __name__== '__main__':
    m = client(host=MASTERHOST, port=MASTERPORT)
    graph = {}
    file = open(r'data\Wiki-Vote-adj.txt', 'r')
    nedges = 0
    for line in file:
        vs = map(int, line.split())
        graph[vs[0]]=vs[1:]
        nedges+=len(vs)-1
    file.close()
    print 'nVertex:', len(graph), 'nEdge:', nedges

    print 'pagerank init'
    curr = m.create_table()
    next = m.create_table()
    g = m.create_table()

    print 'submit graph data'
    m.initpr(graph, g, curr, next)
    print m.get_table(curr)

    print 'pagerank'
    for i in xrange(100):
        a = m.pagerank(g, curr, next, 1)
        print 'ITERATIION ', i, m.get_table(curr)

    m.cleartable(curr)
    m.cleartable(g)
    m.cleartable(next)



