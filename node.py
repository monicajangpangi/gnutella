import socket
import threading
import struct
import binascii
import sys
from functions import parseReceivedMessage, createMessage, ipToNum

neighbours = []
file_repository = {'file': '12345678'}
connections = []
sequence_nr = 0
q_searches = []

#message types
MSG_PING = 0x00
MSG_PONG = 0x01
MSG_BYE = 0x02
MSG_JOIN = 0x03
MSG_QUERY = 0x80
MSG_QHIT = 0x81

def process_message(header, payload, socket):
    global q_searches
    global connections
    global neighbours
    if (header[0] == 1 or header[1] < 1 or header[1] > 5):
        if header[2] == MSG_PING:
            if header[1] == 1:
                print("Respond with PONG A")
                socket.send(createMessage(msg_type=MSG_PONG,
                                          org_port=socket.getsockname()[1],
                                          org_ip=socket.getsockname()[0]))
            else:
                tmp = list(neighbours)
                if (header[6], header[4]) in tmp:
                    tmp.remove((header[6], header[4]))
                response = tmp[:5]
                data = bytearray()
                if(len(response) > 0):
                    data.extend(struct.pack('>HH', len(response), 0))
                    for item in response:
                        data.extend(struct.pack('>IHH', ipToNum(item[0]),
                                                item[1], 0))
                print("Respond with PONG B")
                socket.send(createMessage(msg_type=MSG_PONG,
                                          org_port=socket.getsockname()[1],
                                          org_ip=socket.getsockname()[0],
                                          payload=data))
        elif header[2] == MSG_PONG:
            if len(payload) > 0:
                for i in range(1, len(payload)):
                    if not (payload[i][0], payload[i][1]) in neighbours:
                        neighbours.append((payload[i][0], payload[i][1]))
        elif header[2] == MSG_BYE:
            if (header[6], header[4]) in neighbours:
                neighbours.remove((header[6], header[4]))
            if (header[6], socket) in connections:
                connections.remove((header[6], socket))

        elif header[2] == MSG_JOIN:
            if not (header[6], header[4]) in neighbours:
                neighbours.append((header[6], header[4]))
            data = bytearray()
            data.append(0x02)
            data.append(0x00)
            print("Respond with JOIN OK")
            socket.send(createMessage(msg_type=MSG_JOIN,
                                      org_port=socket.getsockname()[1],
                                      org_ip=socket.getsockname()[0],
                                      payload=data))
        elif header[2] == MSG_QUERY:
            if payload[0] in file_repository:
                k = payload[0]
                print(k)
                data = struct.pack('>HHHH4s', 1, 0, 1, 0,
                                   binascii.unhexlify(file_repository[k]))
                print("Respond with QUERY HIT")
                socket.send(createMessage(msg_type=MSG_QHIT,
                                          org_port=socket.getsockname()[1],
                                          org_ip=socket.getsockname()[0],
                                          payload=data, msg_id=header[7]))
            else:
                q_searches.append((header[6], header[7]))
                forward(header, payload, socket)
        elif header[2] == MSG_QHIT:
            for (x, y) in q_searches:
                if y == header[7]:
                    # Check if connection exists with given ip address
                    for (a, b) in connections:
                        if a == x:
                            data = bytearray()
                            data.extend(struct.pack('>HH', payload[0], 0))
                            for i in range(payload[0]):
                                data.extend(
                                    struct.pack('>HH4s', i+1, 0,
                                                binascii.unhexlify(
                                                    payload[1][i+1])))
                            b.send(createMessage(msg_type=MSG_QHIT,
                                                 org_port=header[4],
                                                 org_ip=header[6],
                                                 payload=data, msg_id=y))

def forward(header, payload, sock):
    global connections
    data = struct.pack('>'+str(len(payload[0]))+'s', payload[0])
    message = createMessage(msg_type=MSG_QUERY, ttl=header[1]-1,
                            org_port=header[4], org_ip=header[6],
                            msg_id=header[7], payload=data)

    for connection in connections:
        if sock is not connection[1]:
            print("Forwarded Message parsed:")
            print(parseReceivedMessage(message))
            connection[1].send(message)


def handshake(socket):
    socket.send(createMessage(msg_type=MSG_JOIN,
                              org_ip=socket.getsockname()[0]))
    response = socket.recv(1024)
    header, payload = parseReceivedMessage(response)
    if payload == ('0200'):
        global neighbours
        neighbours.append((header[6], header[4]))
        socket.send(createMessage(ttl=5, org_port=socket.getsockname()[1],
                                  org_ip=socket.getsockname()[0]))
        response = socket.recv(1024)
        header, payload = parseReceivedMessage(response)
        if len(payload) > 0:
            for i in range(1, len(payload)):
                if not (payload[i][0], payload[i][1]) in neighbours:
                    neighbours.append((payload[i][0], payload[i][1]))


def p2p_replying(socket):
    connected = True
    while connected:
        message = socket.recv(1024)
        print("Message received:")
        print(binascii.hexlify(message))
        header, payload = parseReceivedMessage(message)
        print(header)
        print(payload)
        if(header[2] == MSG_BYE):
            connected = False
        process_message(header, payload, socket)

    socket.close()


def p2p_initiation(socket):
    handshake(socket)
    p2p_replying(socket)


def make_server_socket(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((socket.gethostname(), port))
    print("Can be reached at "+socket.gethostbyname(socket.gethostname()) +
          " port "+str(port))
    s.listen(5)
    return s


def connect_to_another_node(ip, port):
    global connections
    file_repository["xfile"] = "11111111"
    bootstrap = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bootstrap.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    bootstrap.bind((socket.gethostname(), int(sys.argv[1])))
    bootstrap.connect((ip, port))
    connections.append((ip, bootstrap))
    s = make_server_socket(int(sys.argv[1]))
    source = threading.Thread(target=p2p_initiation, args=[bootstrap])
    source.start()
    print('file repository data of this node', file_repository)
    while 1:
        (cl, address) = s.accept()
        connections.append((address[0], cl))
        print("accepted")
        ct = threading.Thread(target=p2p_replying, args=[cl])
        ct.start()

try:
    connect_to_another_node(str(sys.argv[2]), int(sys.argv[3]))
except Exception as e:
    s = make_server_socket(int(sys.argv[1]))
    file_repository["myfile"] = "87654321"
    print('file repository data of this node', file_repository)
    print("Node started")
    while 1:
        (cl, address) = s.accept()
        connections.append((address[0], cl))
        print("accepted")
        ct = threading.Thread(target=p2p_replying, args=[cl])
        ct.start()
        
