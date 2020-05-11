import socket 
import threading
import os
import time
import hashlib


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.addr = (self.host, self.port)
		self.successor = self.addr
		self.predecessor = self.addr
		# additional state variables

	def lookup(self, my_id, key_id):
		return self.successor
		successor_hash = self.hasher(self.successor[0] + str(self.successor[1]))
		if(my_id < successor_hash and successor_hash < key_id):
			successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Establising a connection with successor
			joinsocket.connect(self.successor)
			joinsocket.send(("lookup " + str(key_id)).encode('utf-8')) # Sending key_id
			predecessor = socket.recv(1024) # Waiting for predecessor address
			return predecessor.decode('utf')
		
		else:
			return self.successor

	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''

		msg = client.recv(1024)
		if msg.decode('utf-8').split(" ")[0] == "join":
			if self.successor == self.addr: # corner case 2 nodes
				self.successor = (msg.decode('utf-8').split(" ")[1], int(msg.decode('utf-8').split(" ")[2]))
				self.predecessor = (msg.decode('utf-8').split(" ")[1], int(msg.decode('utf-8').split(" ")[2]))
				client.send("join2".encode('utf-8'))
			else:
				successor = self.lookup(self.hasher(self.addr[0] + str(self.addr[1])), self.hasher(addr[0] + str(addr[1])))
				client.send((successor[0] + " " + str(successor[1])).encode('utf-8'))
		# elif msg.decode('utf-8').split(" ")[0] == "lookup":
		# 	predecessor = self.lookup(self.hasher(self.addr[0] + str(self.addr[1])), int(msg.decode('utf-8').split(" ")[1]))
		client.close()


	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		joinsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Establising a connection with node already present in DHT
		if joiningAddr != "": # corner case 1 node if joiningAddr == ""
			joinsocket.connect(joiningAddr)
			joinsocket.send(("join " + self.addr[0] + " " + str(self.addr[1])).encode('utf-8')) # Sending join msg
			successor = joinsocket.recv(1024).decode('utf-8') # Waiting for predecessor address
			if successor == "join2": # corner case 2 nodes
				self.successor = joiningAddr
				self.predecessor = joiningAddr
			else:
				self.successor = (successor.split(" ")[0], int(successor.split(" ")[1]))
		joinsocket.close()
		# print(self.addr, self.predecessor, self.successor)

	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		
