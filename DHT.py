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
		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)
		# additional state variables
		threading.Thread(target = self.ping).start()

	def ping(self):
		while self.stop == False:
			time.sleep(0.5)
			predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			predecessorsocket.connect(self.successor)
			predecessorsocket.send("predecessor_check".encode('utf-8'))
			predecessor = predecessorsocket.recv(1024).decode('utf-8')
			host = predecessor.split(" ")[0]
			port = int(predecessor.split(" ")[1])
			predecessorsocket.close()
			if (host, port) != (self.host, self.port):
				self.predecessor = (host, port)
				update_successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # send a message to predecessor so it can update its successor
				update_successorsocket.connect(self.predecessor)
				update_successorsocket.send(("successor_update " + self.host + " " + str(self.port)).encode('utf-8'))
				update_successorsocket.close()
				update_predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #send a message to successor so it can update its predecessor
				update_predecessorsocket.connect(self.successor)
				update_predecessorsocket.send(("predecessor_update " + self.host + " " + str(self.port)).encode('utf-8')) 
				update_predecessorsocket.close()


	def lookup(self, key_id):
		n = self.hasher(self.successor[0] + str(self.successor[1]))
		# print(self.key,n,key_id)
		if ((self.key < n) and (n < key_id)) or ((self.key < n) and (key_id < self.key)):
			successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			successorsocket.connect(self.successor)
			successorsocket.send(("lookup " + str(key_id)).encode('utf-8')) # msg sent to lookup condition in handleConnection
			successor = successorsocket.recv(1024).decode('utf-8')
			host = successor.split(" ")[0]
			port = int(successor.split(" ")[1])
			successorsocket.close()
			return (host, port)
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

		msg = client.recv(1024).decode('utf-8')
		msg_type = msg.split(" ")[0]
		if msg_type == "join":
			client_host = msg.split(" ")[1]
			client_port = int(msg.split(" ")[2])
			if self.successor == (self.host, self.port): # corner case 2 nodes
				self.successor = (client_host, client_port)
				self.predecessor = (client_host, client_port)
				client.send("join2".encode('utf-8'))
			else:
				key_id = self.hasher(client_host + str(client_port))
				successor = self.lookup(key_id)
				client.send((successor[0] + " " + str(successor[1])).encode('utf-8')) # msg sent back to join function
		elif msg_type == "lookup":
			key_id = int(msg.split(" ")[1])
			successor = self.lookup(key_id)
			client.send((successor[0] + " " + str(successor[1])).encode('utf-8')) # msg sent to lookup function
		elif msg_type == "predecessor_check":
			host = self.predecessor[0]
			port = str(self.predecessor[1])
			client.send((host +" " + port).encode('utf-8'))
		elif msg_type == "predecessor_update":
			client_host = msg.split(" ")[1]
			client_port = int(msg.split(" ")[2])
			self.predecessor = (client_host, client_port)
		elif msg_type == "successor_update":
			client_host = msg.split(" ")[1]
			client_port = int(msg.split(" ")[2])
			self.successor = (client_host, client_port)
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
			joinsocket.send(("join " + self.host + " " + str(self.port)).encode('utf-8')) # Sending join msg
			successor = joinsocket.recv(1024).decode('utf-8') # Waiting for predecessor address
			if successor == "join2": # corner case 2 nodes
				self.successor = joiningAddr
				self.predecessor = joiningAddr
			else: # more than three nodes
				host = successor.split(" ")[0]
				port = int(successor.split(" ")[1])
				self.successor = (host, port)
		joinsocket.close()
		# print(self.addr, self.key, joiningAddr, self.successor)

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

		
