#!/usr/bin/python
import SocketServer
import json
import time

class MyTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

class MyTCPServerHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            #data = json.loads(self.request.recv(1024).strip())
            # process the data, i.e. print it:
            #print self.request.recv(7024).strip()
            with open('data.txt', 'a') as outfile:
               #json.dump(data, outfile)

               outfile.write(self.request.recv(10024).strip())
               outfile.write("\n")
               #print "Start : %s" % time.ctime()
               #time.sleep( 1130 )
               #print "END : %s" % time.ctime()

            # send some 'ok' back
            self.request.sendall(json.dumps({'return':'ok'}))
        except Exception, e:
            print "Exception wile receiving message: ", e

server = MyTCPServer(('127.0.0.1', 13373), MyTCPServerHandler)
server.serve_forever()
