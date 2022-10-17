import time

STATE_data = None

def setup(hostname, instance):
	global STATE_data

	STATE_data = [
			hostname,
			instance,
	]
	print("Test process-stage setup:", STATE_data)

def run():
	global STATE_data
	
	for i in range(5, 0, -1):
		print("Test process-stage run:", i)
		time.sleep(1)

	return STATE_data

if __name__ == "__main__":
	import socket

	setup(socket.gethostname(), 1)
	print(run())
