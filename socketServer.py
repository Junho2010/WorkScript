import socketserver


class MyStreamServerHandler(socketserver.StreamRequestHandler):
    def handle(self, test):
        data = self.request.recv(2048)
        print(repr(data))
        # data = bytes("Hello," + data, 'utf-8')
        # self.wfile.write(data)


class MixInTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


if __name__ == '__main__':
    HOST, PORT = "127.0.0.1", 8088
    server = MixInTcpServer((HOST, PORT), MyStreamServerHandler)
    with server:
        server.serve_forever()
