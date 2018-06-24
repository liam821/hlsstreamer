#!/usr/bin/env python

'''

hls repeater

Liam Slusser
liam@slacker.com
6/23/2018

'''

import threading, urllib2, httplib, time, re, socket, SocketServer, BaseHTTPServer, math
from collections import deque

m3u8 = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-INDEPENDENT-SEGMENTS"""

class hls:
    def __init__(self,listen_port):
        self.listen_port = listen_port
        self.running = True
        self.sequence = deque()
        self.max_in_sequence = 30
        self.stream_one_m3u8 = ""
        self.stream_two_m3u8 = ""
        self.http_retry = 3
        self.time_between_retry = 1
        self.startinputStream()
        self.segment_length = float(10.0)
        
    class fakeErrorResponse(object):
        def __init__(self):
            self.status = 500
            
    def startinputStream(self):
        ''' this starts the loop in another thread '''
        self._inputStream = threading.Thread(target=self.inputStream)
        self._inputStream.start()
    
    def buildTwom3u8(self,r):
        #self.stream_two_m3u8 = re.search('(^.*?)\n#EXTINF',r,re.M|re.DOTALL).group(1)
        pass
    
    def getOnem3u8(self):
        #return self.stream_one_m3u8 + "\nhttp://localhost:%s/hls/master_%s.m3u8\n" % (self.listen_port,self.stream_bw)
        return self.stream_one_m3u8
    
    def getTwom3u8(self):
        

        r = self.stream_two_m3u8 + "\n"
        if len(self.sequence) <= 10:
            first = 0
            fetch = len(self.sequence)
        else:
            first = len(self.sequence) - 10
            fetch = 10

        r = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:%s
#EXT-X-MEDIA-SEQUENCE:%s
#EXT-X-DISCONTINUITY-SEQUENCE:17
""" % (int(math.ceil(float(self.segment_length))),self.sequence[first]['media_sequence'])

        for segment in range(len(self.sequence)-fetch,len(self.sequence)):
            r += "#EXTINF:%s,\n" % (self.segment_length)
            r += "http://localhost:%s/hls/master_%s_%s.ts\n" % (self.listen_port,self.stream_bw,self.sequence[segment]['seqnum'])
        return r
    
    def doConnRequest(self,conn,method,uri,headers):
        retry = self.http_retry
        print "grabbing %s" % (uri)
        while retry:
            try:
                conn.request(method,uri,headers=headers)
                response = conn.getresponse()
            except:
                return self.fakeErrorResponse()
            if response.status == 200:
                return response
            time.sleep(self.time_between_retry)
            retry -= 1
        print "ERROR [%s]: got http error code %s requesting %s" % (time.ctime(),response.status,uri)

    def inputStream(self):
        '''
        hls input Stream
        '''
        
        host = "nasa-i.akamaihd.net"
        protocol = "https"
        uri = "/hls/live/253565/NASA-NTV1-Public/master.m3u8"
        # if the m3u8 has multiple streams select a specific one to straem
        #stream_bw = "1484010"
        self.stream_bw = 1484010
        
        #url = "https://nasa-i.akamaihd.net/hls/live/253565/NASA-NTV1-Public/master.m3u8"
        method = 'GET'
        
        conn = False
        
        while self.running:
            headers = {}
            headers['Origin'] = "https://www.nasa.gov"
            headers['Referer'] = "https://www.nasa.gov/multimedia/nasatv/index.html"
            headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.79 Safari/537.36'
            # grab base m3u8
            if not conn:
                if protocol == "http":
                    conn = httplib.HTTPConnection(host)
                elif protocol == "https":
                    conn = httplib.HTTPSConnection(host)
            response = self.doConnRequest(conn,method,uri,headers)
            if response.status != 200:
                # close the connection and try again
                conn.close()
                conn = False
            else:
                # we have a 200 parse the stream and grab stream m3u8
                cookie = response.getheader("Set-Cookie","")
                r = response.read()
                stream_url = re.search('.*BANDWIDTH=%s.*\n(.[^\n]*)\n' % (self.stream_bw),r).group(1)
                #new_m3u8 = m3u8 + "\n" + re.search('.*\n(.[^\n]*BANDWIDTH=%s.[^\n]*\n.[^\n]*)\n' % (stream_bw),a).group(1)
                # generate the m3u8 for our clients
                self.stream_one_m3u8 = m3u8 + re.search('.*\n(.[^\n]*BANDWIDTH=%s.[^\n]*)\n' % (self.stream_bw),r).group(1) + "\nhttp://localhost:%s/hls/master_%s.m3u8\n" % (self.listen_port,self.stream_bw)
                # how grab the actual m3u8 for the specific stream we want
                if cookie:
                    headers['Cookie'] = cookie
                
                self.segment_length = float(10.0)
                while conn:
                    st = time.time()
                    response = self.doConnRequest(conn,method,stream_url,headers)
                    if response.status != 200:
                        # close the connection and try again
                        conn.close()
                        conn = False
                    else:
                        cookie = response.getheader("Set-Cookie","")
                        r = response.read()
                        if cookie:
                            headers['Cookie'] = cookie
                        media_sequence = int(re.search('EXT-X-MEDIA-SEQUENCE:(\d+)',r).group(1))
                        
                        self.buildTwom3u8(r)
                        segments = re.findall('^http.[^\n]*ts',r,re.M|re.DOTALL)
                        
                        if len(self.sequence) == 0:
                            # this is our first request - grab everything and fill the sequence buffer
                            self.segment_length = float(re.search('EXTINF:(.[^,]*)',r).group(1))
                            #for segment in segments:
                            for n in range(0,len(segments)):
                                seqnum = re.search('(\d+)\.ts$',segments[n]).group(1)
                                response = self.doConnRequest(conn,method,segments[n],headers=headers)
                                if response.status == 200:
                                    cookie = response.getheader("Set-Cookie","")
                                    if cookie:
                                        headers['Cookie'] = cookie
                                    print "appending seqnum %s media_sequence %s" % (seqnum,media_sequence+n)
                                    self.sequence.append( {'payload':response.read(),'seqnum':seqnum,'media_sequence':media_sequence+n} )
                        else:
                            # search for the last segment we have, and grab
                            grabList = []
                            current_end_seq = self.sequence[-1]['seqnum']
                            if current_end_seq == re.search('(\d+)\.ts$',segments[-1]).group(1):
                                # already have, wait 5 seconds and try again
                                time.sleep(5)
                            else:
                                found = False
                                for n in range(0,len(segments)):
                                    if found:
                                        seqnum = re.search('(\d+)\.ts$',segments[n]).group(1)
                                        response = self.doConnRequest(conn,method,segments[n],headers=headers)
                                        if response.status == 200:
                                            cookie = response.getheader("Set-Cookie","")
                                            if cookie:
                                                headers['Cookie'] = cookie
                                            print "appending seqnum %s media_sequence %s" % (seqnum,media_sequence+n)
                                            self.sequence.append( {'payload':response.read(),'seqnum':seqnum,'media_sequence':media_sequence+n} )          
                                    elif current_end_seq == re.search('(\d+)\.ts$',segments[n]).group(1):
                                        found = True

                    # clean up sequence
                    while len(self.sequence) > self.max_in_sequence:
                        self.sequence.popleft()
                    
                    # sleep and loop
                    print "napping"
                    while time.time()-st <= self.segment_length:
                        time.sleep(0.25)
                    print "sequene len: %s" % (len(self.sequence))
        return True

              
class webServer():
    def __init__(self,hls,listen_port):
        self.webHandler.hls = hls
        self.hls = hls
        self.webserver = {}
        self.number_of_http_threads = 10
        self.addr = ('', listen_port)
        self.sock = socket.socket (socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.addr)
        self.sock.listen(5)
        self.start()
        
    class webHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        hls = None
        protocol_version = 'HTTP/1.1'
        def do_GET(self):
            if len(self.hls.stream_one_m3u8) == 0:
                self.send_error(404, "Object not found")
            elif self.path == "/hls/master.m3u8":
                r = self.hls.getOnem3u8()
                self.send_response(200)
                self.send_header('Content-type', 'application/x-mpegURL')
                self.send_header('Access-Control-Allow-Headers','origin,range,hdntl,hdnts')
                self.send_header('Access-Control-Expose-Headers','Server,range,hdntl,hdnts')
                self.send_header('Access-Control-Allow-Methods','GET, HEAD, OPTIONS')
                self.send_header('Access-Control-Allow-Credentials','true')
                self.send_header('Access-Control-Allow-Origin','*')
                self.send_header('Content-Length',len(r))
                self.end_headers()
                self.wfile.write(r)
            elif self.path == "/hls/master_%s.m3u8" % (self.hls.stream_bw):
                r = self.hls.getTwom3u8()
                self.send_response(200)
                self.send_header('Content-type', 'application/x-mpegURL')
                self.send_header('Access-Control-Allow-Headers','origin,range,hdntl,hdnts')
                self.send_header('Access-Control-Expose-Headers','Server,range,hdntl,hdnts')
                self.send_header('Access-Control-Allow-Methods','GET, HEAD, OPTIONS')
                self.send_header('Access-Control-Allow-Credentials','true')
                self.send_header('Access-Control-Allow-Origin','*')
                self.send_header('Content-Length',len(r))
                self.end_headers()
                self.wfile.write(r)
            elif "/hls/master_%s_" % (self.hls.stream_bw) in self.path:
                segnum = re.search('_(\d+)\.ts',self.path).group(1)
                for segment in self.hls.sequence:
                    if segment['seqnum'] == segnum:
                        self.send_response(200)
                        self.send_header('Content-type', 'video/MP2T')
                        self.send_header('Access-Control-Allow-Headers','origin,range,hdntl,hdnts')
                        self.send_header('Access-Control-Expose-Headers','Server,range,hdntl,hdnts')
                        self.send_header('Access-Control-Allow-Methods','GET, HEAD, OPTIONS')
                        self.send_header('Access-Control-Allow-Credentials','true')
                        self.send_header('Access-Control-Allow-Origin','*')
                        self.send_header('Content-Length',len(segment['payload']))
                        self.end_headers()
                        self.wfile.write(segment['payload'])
                        return
                # oops, could not find that seq number
                print "segment not found 404"
                self.send_error(404, "Object not found")
            else:
                self.send_error(404, "Object not found")
                return

    class Thread(threading.Thread):
        def __init__(self, i, myself):
            self.myself = myself
            threading.Thread.__init__(self)
            self.i = i
            self.daemon = True
            self.start()
        def run(self):
            self.myself.webserver[self.i] = BaseHTTPServer.HTTPServer(self.myself.addr, self.myself.webHandler, False)
    
            self.myself.webserver[self.i].socket = self.myself.sock
            self.myself.webserver[self.i].server_bind = self.server_close = lambda self: None
            self.myself.webserver[self.i].serve_forever()
    
    def start(self):
        [self.Thread(i,self) for i in range(self.number_of_http_threads)]
        while self.hls.running:
            time.sleep(1)
        for i in range(self.number_of_http_threads):
            self.webserver[i].shutdown()
        return True

class main:
    def __init__(self):
        listen_port = 8000
        self.hls = hls(listen_port)

        self.webServer = webServer(self.hls,listen_port)


if __name__ == "__main__":
    m = main()
