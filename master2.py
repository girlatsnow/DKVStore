__author__ = 'licui'
__email__ = 'cui.judy.lee@gmail.com'
# created on 2015/7/3 11:17
#coding: utf-8
import rpyc
import threading
import logging
from rpyc.utils.server import ThreadedServer




WorkerStatus = {} #0=IDLE, 1=BUSY, 2=WAITING
WorkingList = []
WorkerSockets = {}  #worker: sock




logging.basicConfig(level=logging.INFO)


class MworkerService(rpyc.Service):
    def on_connect(self):
        pass
    def on_disconnect(self):
        if (self.host, self.port) in WorkingList:
            WorkingList.remove((self.host, self.port))

    def exposed_getworkerstatus(self, host, port):
        self.host = host
        self.port = port
        if not (host,port) in WorkerStatus:
            WorkerStatus[(host, port)]=0
        return WorkerStatus[(host, port)]

    def exposed_connectwk(self, host, port):
        self.sock = rpyc.connect(host, port, config={"allow_all_attrs": True,  "allow_pickle":True})
        WorkerSockets[(host, port)]= self.sock
        WorkingList.append((host, port))

class MclientService(rpyc.Service):
    #socks = []
    worklist = []

    def on_connect(self):
        self.worklist=[]
        for w in WorkingList:
            self.worklist.append(w)
        logging.info('total workers: '+str(len(self.worklist)))

    def on_disconnect(self):
        for w in self.worklist:
            WorkerStatus[w]=0

    def getsock(self, i):
        return WorkerSockets[self.worklist[i%len(self.worklist)]]

    def exposed_get(self,i,a):
        res =  self.getsock(a).root.get(i,a)
        logging.info('get %d', res)
        return res

    def exposed_set(self,i, a, b):
        return self.getsock(a).root.set(i,a, b)

    def exposed_update(self,i, k, delta):
        return self.getsock(k).root.update(i, k, delta)

    def exposed_update_table2(self, frm, i, to, j):
        return self.getsock(i).root.update_table2(frm, i, to, j)

    def exposed_create_table(self):
        for s in [WorkerSockets[w] for w in self.worklist]:
            i = s.root.createtable()
        return i

    def exposed_cleartable(self, i):
        cnt=0
        for s in [WorkerSockets[w] for w in self.worklist]:
            cnt +=  s.root.cleartable(i)
        return cnt

    def exposed_swaptable(self, frm, to):
        cnt = 0
        for s in [WorkerSockets[w] for w in self.worklist]:
            cnt +=  s.root.swaptable(frm, to)
        return cnt

    def exposed_pagerank(self, graph, curr, next, factor):

        self.pr = 0
        self.resnext = {}

        # collect next -> res
        logging.info('pagerank')

        for w in self.worklist:
            #s = WorkerSockets[w]
            WorkerStatus[w]= 1
            t = threading.Thread(target = self.t_pr, args=(w, graph, curr, next,factor,))
            t.start()

        while self.pr<len(self.worklist):
            pass

        # swap res -> curr
        logging.info('swap table')
        self.exposed_settable(curr, self.resnext, True)
        return 1

    def t_pr(self, w, graph, curr, next, factor):
        logging.info('thread pr')

        while (True):
            try:
                apr = rpyc.async( WorkerSockets[w].root.pagerank)
                tmp = apr(graph, curr, next, factor)
                tmp.set_expiry(120)
                tmp.wait()
                for vid, rank in  tmp.value.iteritems():
                    if vid in self.resnext:
                        self.resnext[vid]+=rank
                    else:
                        self.resnext[vid]=rank
            except Exception as e:
                    pass
            else:
                if tmp.expired:
                    pass
                else:
                    break



        self.pr+=1
        logging.info('thread pr finish')

    def t_settable(self, w, i, curr, table, RESTORE=False):
        logging.info('thread set table')
        tmp={}
        for x, r in table.iteritems():
            if x%len(self.worklist)==i:
                tmp[x]=r


        why = None
        while (True):
            try:
                why = WorkerSockets[w].root.settable(curr, tmp)
            except Exception as e:
                pass
            else:
                if why is None:
                    pass
                else:
                    break
        self.st += why
        logging.info('thread set table finish')
        if RESTORE:
            WorkerSockets[w].root.restore(curr)

    def exposed_settable(self, to, table, RESTORE):
        self.st = 0
        for i,w in enumerate(self.worklist):
            WorkerStatus[w]=2
            #s=WorkerSockets[w]
            t = threading.Thread(target = self.t_settable, args=(w, i, to, table, RESTORE, ))
            t.start()
        while self.st<len(self.worklist):
            pass

        return 1


    def t_initpr(self, w, graphid, currid, nextid):
        aipr = rpyc.async(WorkerSockets[w].root.initpr)
        res = aipr(graphid, currid, nextid)
        while not res.ready:
            pass
        self.ipr+=res.value

    def exposed_initpr(self, graphdata, gtable, currtable, nexttable):
        self.exposed_settable(gtable, graphdata, False)
        self.ipr = 0
        print 'cnt of workers:', len(self.worklist)
        for w in self.worklist:
            t = threading.Thread(target = self.t_initpr, args=(w, gtable, currtable, nexttable))
            t.start()
        while self.ipr<len(self.worklist):
            pass

        return 1

    def t_gettable(self, w, i):
        logging.info('thread get table')
        tmp = None
        while (True):
            try:
                tmp = WorkerSockets[w].root.gettable(i)
                cnt = len(tmp)
                self.gtres.update(tmp)
            except Exception as e:
                pass
            else:
                break

        self.gt+=1
        logging.info('thread gettable finish')

    def exposed_gettable(self, i):
        self.gtres = {}
        self.gt = 0
        for w in self.worklist:
            t = threading.Thread(target=self.t_gettable, args=(w,i,))
            t.start()

        while self.gt<len(self.worklist):
            pass
        return self.gtres

def runserver(Sv, prt):
    masterforworker = ThreadedServer( Sv, port = prt,protocol_config={"allow_all_attrs":True, "allow_pickle":True},listener_timeout=20000)
    masterforworker.start()

if __name__ == "__main__":
    workermanager = threading.Thread(target = runserver, args=(MworkerService, 5051, ))
    workermanager.start()
    clientmanager = threading.Thread(target = runserver, args=(MclientService, 5050, ))
    clientmanager.start()






