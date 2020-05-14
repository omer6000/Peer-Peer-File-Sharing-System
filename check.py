from DHT import Node
import time
import os
import sys
import random
import uuid
import shutil  


def generateFiles(files):
	for f in files:
		file = open(f,"w")
		lowercase_str = uuid.uuid4().hex  
		file.write(lowercase_str.upper())
		file.close()

def removeFiles(file):
	for f in files:
		os.remove(f)

def initiate(p):
	print("\n \t --* Testing Initialization *--\n")
	nodes = []
	points = 0
	try:
		n1 = Node("localhost", p[0])
		nodes.append(n1)
		n2 = Node("localhost", p[1])
		nodes.append(n2)
		n3 = Node("localhost", p[2])
		nodes.append(n3)
		n4 = Node("localhost", p[3])
		nodes.append(n4)
		n5 = Node("localhost", p[4])
		nodes.append(n5)
	except Exception as e:
		print ("RunTime Error in node initialization! Following exception occurred:")
		print (e)
		return
	if nodes[0].successor == ("localhost",p[0]) and nodes[0].predecessor == ("localhost",p[0]):
		print ("Initialization Successful. (+1)")
		points += 1
	else:
		print ("Wrong initialization!")
	print ("\nInitialization testing completed. Points:",points,"/ 1")
	return nodes, points

def testJoin(nodes,p):
	print ("\n\t --* Testing Join *--\n")
	print ("Case 1: Checking for corner case of 1 node.")
	points = 0
	nodes[0].join("")
	time.sleep(2)
	if nodes[0].successor == ("localhost",p[0]) and nodes[0].predecessor == ("localhost",p[0]):
		print ("Case 1 passed. \t(+1)")
		points += 1
	else:
		print ("Case 1 failed. \t(0)")
	print("Case 2: Checking for corner case of 2 nodes.")
	nodes[1].join(("localhost", p[0]))
	time.sleep(2)
	if nodes[1].successor == ("localhost",p[0]) and nodes[1].predecessor == ("localhost",p[0]) and nodes[0].successor == ("localhost",p[1]) and nodes[0].predecessor == ("localhost",p[1]):
		print ("Case 2 passed. \t(+3)")
		points += 3
	else:
		print ("Case 2 failed. \t(0)")
	print ("Case 3: Checking for general case.")
	nodes[2].join(("localhost", p[0]))
	time.sleep(2)
	nodes[3].join(("localhost", p[0]))
	time.sleep(2)
	nodes[4].join(("localhost", p[1]))
	time.sleep(2)
	nodes.sort(key=lambda x: x.key, reverse=False)
	correct = True
	for i in range(len(nodes)):
		if nodes[i].successor == None:
			correct = False
		elif nodes[i].successor[1] == nodes[(i+1) % len(nodes)].port and nodes[i].predecessor[1] == nodes[i-1].port:
			continue
		else:
			correct = False
	if correct:
		print ("Case 3 passed. \t(+5)")
		points += 5
	else:
		print ("Case 3 failed. \t(0)")
	print ("\nJoin testing completed. Points:",points,"/ 9")
	return nodes, points

def testPutandGet(nodes, files):
	print ("\n\t --* Testing Put and Get *--\n")
	print ("Placing files on DHT.")
	points = 0
	fileHashes = []
	for i in range(len(files)):
		fileHashes.append(nodes[0].hasher(files[i]))
		nodes[0].put(files[i])
	time.sleep(1)
	print ("Testing Put.")
	correct = True
	for i in range(len(files)):
		for j in range(len(nodes)):
			if (fileHashes[i] <= nodes[j].key and fileHashes[i] > nodes[j-1].key) or (fileHashes[i] > nodes[-1].key and j == 0):
				if files[i] not in nodes[j].files:
					correct = False
	if correct:
		print ("All files put successfully. \t(+7)")
		points += 7
	else:
		print ("Some or all files put incorrectly. \t(0)")
		return 0
	print ("Testing Get.")
	removeFiles(files)
	time.sleep(4)
	print ("Checking for a file placed on DHT.")
	if nodes[0].get(files[0]) == None:
		print ("Could not retrieve a file placed on DHT.")
		print ("Get failed. \t(0)")
		return points
	print ("Checking for files not put originally.")
	if nodes[0].get("absent.txt") != None:
		print ("File retrieved against a key that was not placed on DHT.")
		print ("Get failed. \t(0)")
		return points
	print ("All files retrieved successfully. \t(+3)")
	points += 3
	print ("\nPut and Get testing completed. Points: 10 / 10")
	# print("Port:",nodes[0].port, "Key:",nodes[0].key, "Files:", list(map(nodes[0].hasher,nodes[0].files)))
	# print("Port:",nodes[1].port, "Key:",nodes[1].key, "Files:", list(map(nodes[1].hasher,nodes[1].files)))
	# print("Port:",nodes[2].port, "Key:",nodes[2].key, "Files:", list(map(nodes[2].hasher,nodes[2].files)))
	# print("Port:",nodes[3].port, "Key:",nodes[3].key, "Files:", list(map(nodes[3].hasher,nodes[3].files)))
	# print("Port:",nodes[4].port, "Key:",nodes[4].key, "Files:", list(map(nodes[4].hasher,nodes[4].files)))
	
	return points

def testFileRehashing(nodes, files, sp):
	print ("\n\t --* Testing File Rehashing *--\n")
	ps = [sp+0, sp+1, sp+2]
	print ("New nodes joining network.")
	for p in ps:
		n1 = Node("localhost", p)
		n1.join((nodes[0].host, nodes[0].port))
		nodes.append(n1)
		time.sleep(2)
	nodes.sort(key=lambda x: x.key, reverse=False)
	# print(nodes[0].key, nodes[1].key, nodes[2].key, nodes[3].key, nodes[4].key, nodes[5].key, nodes[6].key, nodes[7].key)
	correct = True
	print ("Checking if files rehashed correctly.")
	# print("Port:",nodes[0].port, "Key:",nodes[0].key, "Files:", list(map(nodes[0].hasher,nodes[0].files)),list(map(nodes[0].hasher,nodes[0].backUpFiles)),nodes[0].hasher(nodes[0].predecessor[0] + str(nodes[0].predecessor[1])),nodes[0].hasher(nodes[0].successor[0] + str(nodes[0].successor[1])))
	# print("Port:",nodes[1].port, "Key:",nodes[1].key, "Files:", list(map(nodes[1].hasher,nodes[1].files)),list(map(nodes[1].hasher,nodes[1].backUpFiles)),nodes[1].hasher(nodes[1].predecessor[0] + str(nodes[1].predecessor[1])),nodes[1].hasher(nodes[1].successor[0] + str(nodes[1].successor[1])))
	# print("Port:",nodes[2].port, "Key:",nodes[2].key, "Files:", list(map(nodes[2].hasher,nodes[2].files)),list(map(nodes[2].hasher,nodes[2].backUpFiles)),nodes[2].hasher(nodes[2].predecessor[0] + str(nodes[2].predecessor[1])),nodes[2].hasher(nodes[2].successor[0] + str(nodes[2].successor[1])))
	# print("Port:",nodes[3].port, "Key:",nodes[3].key, "Files:", list(map(nodes[3].hasher,nodes[3].files)),list(map(nodes[3].hasher,nodes[3].backUpFiles)),nodes[3].hasher(nodes[3].predecessor[0] + str(nodes[3].predecessor[1])),nodes[3].hasher(nodes[3].successor[0] + str(nodes[3].successor[1])))
	# print("Port:",nodes[4].port, "Key:",nodes[4].key, "Files:", list(map(nodes[4].hasher,nodes[4].files)),list(map(nodes[4].hasher,nodes[4].backUpFiles)),nodes[4].hasher(nodes[4].predecessor[0] + str(nodes[4].predecessor[1])),nodes[4].hasher(nodes[4].successor[0] + str(nodes[4].successor[1])))
	# print("Port:",nodes[5].port, "Key:",nodes[5].key, "Files:", list(map(nodes[5].hasher,nodes[5].files)),list(map(nodes[5].hasher,nodes[5].backUpFiles)),nodes[5].hasher(nodes[5].predecessor[0] + str(nodes[5].predecessor[1])),nodes[5].hasher(nodes[5].successor[0] + str(nodes[5].successor[1])))
	# print("Port:",nodes[6].port, "Key:",nodes[6].key, "Files:", list(map(nodes[6].hasher,nodes[6].files)),list(map(nodes[6].hasher,nodes[6].backUpFiles)),nodes[6].hasher(nodes[6].predecessor[0] + str(nodes[6].predecessor[1])),nodes[6].hasher(nodes[6].successor[0] + str(nodes[6].successor[1])))
	# print("Port:",nodes[7].port, "Key:",nodes[7].key, "Files:", list(map(nodes[7].hasher,nodes[7].files)),list(map(nodes[7].hasher,nodes[7].backUpFiles)),nodes[7].hasher(nodes[7].predecessor[0] + str(nodes[7].predecessor[1])),nodes[7].hasher(nodes[7].successor[0] + str(nodes[7].successor[1])))
	for i in range(len(files)):
		for j in range(len(nodes)):
			if nodes[j].hasher(files[i]) <= nodes[j].key and nodes[j].hasher(files[i]) > nodes[j-1].key or nodes[j].hasher(files[i]) > nodes[-1].key and i == 0:
				if files[i] not in nodes[j].files:
					correct = False
	
	if correct:
		print ("All files rehashed successfully. \t(+5)")
	else:
		print ("Some or all files have been rehashed incorrectly. \t(0)")
		return nodes, 0
	print("\nFile Rehashing testing completed. Points: 5 / 5")
	return nodes, 5

		

def testLeave(nodes, files):
	print ("\n\t --* Testing Leave *--\n")
	ind = 0
	for i in range(len(nodes)):
		if files[0] in nodes[i].files:
			ind = i
	print ("Calling leave function on a node.")
	nodes[ind].leave()
	time.sleep(2)
	del nodes[ind]
	correct = True
	print ("Checking for successor and predecessor updation.\n")
	for i in range(len(nodes)):
		if nodes[i].successor == None:
			correct = False
		elif nodes[i].successor[1] == nodes[(i+1) % len(nodes)].port and nodes[i].predecessor[1] == nodes[i-1].port:
			continue
		else:
			correct = False
	if correct:
		print ("Successor and Predecessor updated successfully for all nodes.\t(+3)")
	else:
		print ("Wrong successor and predecessor returned.\t(0)")
		return nodes, 0
	print ("Checking for file transfer.\n")
	if files[0] in nodes[ind % len(nodes)].files:
		print ("Files transferred correctly.\t(+2)")
	else:
		print ("Files updated incorrectly. \t(0)")
		return nodes, 3
	print("\nLeave testing completed. Points: 5 / 5")
	return nodes, 5

def kill(nodes):
	for n in nodes:
		n.kill()

def testFailureTolerance(nodes, files):
	print ("\n\t --* Testing Failure Tolerance *--\n")
	ind = 0
	for i in range(len(nodes)):
		# print("Port:",nodes[i].port, "Key:",nodes[i].key, "Files:", list(map(nodes[i].hasher,nodes[i].files)),list(map(nodes[i].hasher,nodes[i].backUpFiles)),
		# nodes[i].hasher(nodes[i].predecessor[0] + str(nodes[i].predecessor[1])),
		# nodes[i].hasher(nodes[i].grandsuccessor[0] + str(nodes[i].grandsuccessor[1])))
		if files[0] in nodes[i].files:
			ind = i
	print ("Killing a node.")
	nodes[ind].kill()
	time.sleep(3)
	del nodes[ind]
	correct = True
	for i in range(len(nodes)):
		# print("Port:",nodes[i].port, "Key:",nodes[i].key, "Files:", list(map(nodes[i].hasher,nodes[i].files)),list(map(nodes[i].hasher,nodes[i].backUpFiles)),nodes[i].hasher(nodes[i].predecessor[0] + str(nodes[i].predecessor[1])),nodes[i].hasher(nodes[i].successor[0] + str(nodes[i].successor[1])))
		if nodes[i].successor == None:
			correct = False
		elif nodes[i].successor[1] == nodes[(i+1) % len(nodes)].port and nodes[i].predecessor[1] == nodes[i-1].port:
			continue
		else:
			correct = False
	if correct:
		print ("Successor and Predecessor updated successfully for all nodes.\t(+3)")
	else:
		print ("Wrong successor and predecessor returned.\t(0)")
		return nodes, 0
	print ("Checking for file transfer.\n")
	if files[0] in nodes[ind % len(nodes)].files:
		print ("Files placed at right node after failure.\t(+7)")
	else:
		print ("Files recovery failed. \t(0)")
		return nodes, 3
	print("\nFailure Tolerance testing completed. Points: 10 / 10")
	return nodes, 10


def printN(nodes):
	print ("\nPrinting all nodes.")
	for n in nodes:
		print (n.host, n.port, n.key)
		print (n.successor, n.predecessor, n.secondSuccessor)
		print (n.backUpFiles)
		print (n.files)
		print ([n.hasher(x) for x in n.files])
		print ("--------------------------------------------")

try:
	start_port = int(sys.argv[1])
except:
	print ("Run this script file as 'python check.py <port>' (where 1000 < port < 65500).")
	os._exit(1)

p = [start_port+0, start_port+1, start_port+2, start_port+3, start_port+4]
files = ["dummy.txt", "dummy2.txt","dummy3.txt","dummy4.txt","dummy5.txt","dummy6.txt","dummy7.txt","dummy8.txt"]


nodes, p1 = initiate(p)
nodes, p2 = testJoin(nodes, p)
generateFiles(files)
p3 = testPutandGet(nodes, files)
nodes, p4 = testFileRehashing(nodes, files, start_port+5)
nodes, p5 = testLeave(nodes, files)
nodes, p6 = testFailureTolerance(nodes, files)

print ("\nTotal points: ", p1+p2+p3+p4+p5+p6, "/ 40")

path = "./"
files = os.listdir(path)
for f in files:
    if "dummy" in f:
        os.remove(os.path.join(path, f))
    if "localhost" in f:
        shutil.rmtree(os.path.join(path, f))

os._exit(1)
