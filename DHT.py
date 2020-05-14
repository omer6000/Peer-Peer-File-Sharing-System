import socket 
import threading
import os
import time
import hashlib
from json import dumps, loads

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
		self.grandsuccessor = (self.host, self.port)
		os.mkdir("Backup_" + self.host + "_" + str(self.port))
	
	def update_successor(self):
		update_predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #send a message to successor so it can update its predecessor
		update_predecessorsocket.connect(self.successor)
		update_predecessorsocket.send(("predecessor_update " + self.host + " " + str(self.port)).encode('utf-8')) 
		update_predecessorsocket.close()
		mergebackupsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		mergebackupsoc.connect(self.successor)
		mergebackupsoc.send("MergeBackup".encode('utf-8'))
		mergebackupsoc.close()
	def ping(self):
		while self.stop == False:
			time.sleep(0.5)
			predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			execute = True
			try:
				predecessorsocket.connect(self.successor)
			except:
				predecessorsocket.close()
				self.successor = self.grandsuccessor
				self.update_successor()
				execute = False
			if execute == True:
				predecessorsocket.send("predecessor_check".encode('utf-8'))
				predecessor = predecessorsocket.recv(1024).decode('utf-8')
				predecessorsocket.close()
				host = predecessor.split(" ")[0]
				port = int(predecessor.split(" ")[1])
				for filename in self.files:
					backupsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					try:
						backupsoc.connect(self.successor)
						file_dict = {"files":self.files}
						backupsoc.send(("backup " + filename + " " + dumps(file_dict)).encode('utf-8'))
						backupsoc.close()
					except:
						backupsoc.close()
						self.update_successor()
				if (host, port) != (self.host, self.port):
					self.predecessor = (host, port)
					update_successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # send a message to predecessor so it can update its successor
					update_successorsocket.connect(self.predecessor)
					update_successorsocket.send(("successor_update " + self.host + " " + str(self.port)).encode('utf-8'))
					update_successorsocket.close()
					update_predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #send a message to successor so it can update its predecessor
					try:
						update_predecessorsocket.connect(self.successor)
						update_predecessorsocket.send(("predecessor_update " + self.host + " " + str(self.port)).encode('utf-8')) 
						update_predecessorsocket.close()
					except:
						update_predecessorsocket.close()
						self.update_successor()
					self.rehash()
					grandsucc_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					grandsucc_soc.connect(self.successor)
					grandsucc_soc.send("GrandSuccessor ".encode('utf-8'))
					grandsucc = grandsucc_soc.recv(1024).decode('utf-8')
					grandsucc_host = grandsucc.split(" ")[0]
					grandsucc_port = int(grandsucc.split(" ")[1])
					self.grandsuccessor = (grandsucc_host, grandsucc_port)
					# predecessorsocket.close()
				# predecessorsocket.close()

	def lookup(self, key_id):
		n = self.hasher(self.successor[0] + str(self.successor[1]))
		if ((key_id > self.key) and (key_id < n)): # successor has highest value and key being inserted has middle value
			return self.successor
		elif(self.key > n) and ((key_id < self.key) and (key_id < n)): # successor has smallest value and key being inserted has midddle value
			return self.successor
		elif(self.key > n) and ((key_id > self.key) and (key_id > n)): # successor has smallest value and key being inserted has the highest value
			return self.successor
		elif key_id == self.key:
			return (self.host, self.port)
		elif key_id == n:
			return self.successor
		else:
			successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			successorsocket.connect(self.successor)
			successorsocket.send(("lookup " + str(key_id)).encode('utf-8')) # msg sent to lookup condition in handleConnection
			successor = successorsocket.recv(1024).decode('utf-8')
			host = successor.split(" ")[0]
			port = int(successor.split(" ")[1])
			successorsocket.close()
			return (host, port)


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
		if msg_type == "join": # join request is received here
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
		elif msg_type == "lookup": # lookup request from the lookup function is received here
			key_id = int(msg.split(" ")[1])
			successor = self.lookup(key_id)
			client.send((successor[0] + " " + str(successor[1])).encode('utf-8')) # msg sent to lookup function
		elif msg_type == "predecessor_check": # ping function is requesting for predecessor
			host = self.predecessor[0]
			port = str(self.predecessor[1])
			client.send((host +" " + port).encode('utf-8'))
		elif msg_type == "predecessor_update":  # ping function is telling you to update your predecessor
			client_host = msg.split(" ")[1]
			client_port = int(msg.split(" ")[2])
			self.predecessor = (client_host, client_port)
		elif msg_type == "successor_update":  # ping function is telling you to update your sucessor
			client_host = msg.split(" ")[1]
			client_port = int(msg.split(" ")[2])
			self.successor = (client_host, client_port)
		elif msg_type == "putfile":
			filename = msg.split(" ")[1]
			directory = self.host + "_" + str(self.port) + "/" + filename
			client.send("filename received".encode('utf-8'))
			if filename not in self.files:
				self.files.append(filename)
			self.recieveFile(client, directory)
		elif msg_type == "backup":
			filename = msg.split(" ",2)[1]
			file_dict = loads(msg.split(" ",2)[2])
			self.backUpFiles = file_dict["files"]
			# directory = "Backup_" + self.host + "_" + str(self.port) + "/" + filename
			# client.send("filename received".encode('utf-8'))
			# self.recieveFile(client, directory)
		elif msg_type == "fileexist":
			filename = msg.split(" ")[1]
			directory = self.host + "_" + str(self.port) + "/" + msg.split(" ")[1]
			if(filename in self.files):
				client.send("present".encode('utf-8'))
				host = msg.split(" ")[2]
				port = int(msg.split(" ")[3])
				getsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				getsocket.connect((host, port))
				getsocket.send(("putfile " + filename).encode('utf-8'))
				getsocket.recv(1024)
				self.sendFile(getsocket, directory)
				getsocket.close()
			else:
				client.send("absent".encode('utf-8'))
		elif msg_type == "rehash":
			# loop over file folder
				# Rehash all those names and do lookup over those keys
				# Establish a connection with the node
				# Send file to the new node
				# Delete file
			backUpFiles = self.files
			self.files = []
			self.backUpFiles = []
			for filename in backUpFiles:
				directory = self.host + "_" + str(self.port) + "/" + filename
				file_key = self.hasher(filename)
				newnode = self.lookup(file_key)
				rehashsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				rehashsocket.connect(newnode)
				rehashsocket.send(("putfile " + filename).encode('utf-8'))
				rehashsocket.recv(1024)
				self.sendFile(rehashsocket, directory)
				rehashsocket.close()
			# deletedfiles = list(set(backUpFiles) - set(self.files))
			# for filename in deletedfiles:
			# 	directory = self.host + "_" + str(self.port) + "/" + filename
			# 	os.remove(directory)
		elif msg_type == "send_successor": # put function is requesting for successor
			host = self.successor[0]
			port = str(self.successor[1])
			client.send((host + " " + port).encode('utf-8'))
		elif msg_type == "GrandSuccessor":
			host = self.successor[0]
			port = str(self.successor[1])
			client.send((host + " " + port).encode('utf-8'))
		elif msg_type == "MergeBackup":
			for filename in self.backUpFiles:
				self.files.append(filename)
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
	
	def rehash(self):
		successorsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		successorsoc.connect(self.successor)
		successorsoc.send("rehash ".encode('utf-8'))	
		successorsoc.close()	

	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
	#	[3269, 20020, 12032, 13035, 12498, 51578, 56859, 33174] filehashes
		file_key = self.hasher(fileName) # hash key of file
		addr = self.lookup(file_key) # addr where we have to place the file
		filesocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		filesocket.connect(addr)
		filesocket.send(("putfile " + fileName).encode('utf-8')) # sending file
		filesocket.recv(1024)
		# print("1",fileName)
		self.sendFile(filesocket, fileName)
		filesocket.close()

		# getsuccessorsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# getsuccessorsoc.connect(addr)
		# getsuccessorsoc.send("send_successor".encode('utf-8'))
		# successor = getsuccessorsoc.recv(1024).decode('utf-8')
		# getsuccessorsoc.close()

		# successorsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Establishing a connection with the successor of the node where we are placing files
		# successorsoc.connect((successor.split(" ")[0], int(successor.split(" ")[1])))
		# successorsoc.send(("backup " + fileName).encode('utf-8'))
		# successorsoc.recv(1024)
		# print("1",fileName)
		# self.sendFile(filesocket, fileName)
		# successorsoc.close()
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''

		file_key = self.hasher(fileName)
		addr = self.lookup(file_key)
		filesocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		filesocket.connect(addr)
		filesocket.send(("fileexist " + fileName + " " + self.host + " " + str(self.port)).encode('utf-8'))
		exist = filesocket.recv(1024).decode('utf-8')
		if exist == "absent":
			filesocket.close()
			return None
		else:
			filesocket.close()
			return fileName

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		successor = self.successor
		predecessor = self.predecessor
		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)
		predecessorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		predecessorsocket.connect(predecessor)
		predecessorsocket.send(("successor_update " + successor[0] + " " + str(successor[1])).encode('utf-8'))
		predecessorsocket.close()
		successorsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		successorsocket.connect(successor)
		successorsocket.send(("predecessor_update " + predecessor[0] + " " + str(predecessor[1])).encode('utf-8'))
		successorsocket.close()

		for filename in self.files:
			filesocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			filesocket.connect(successor)
			filesocket.send(("putfile " + filename).encode('utf-8')) # sending file
			filesocket.recv(1024)
			directory = self.host + "_" + str(self.port) + "/" + filename
			self.sendFile(filesocket, directory)
			filesocket.close()


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