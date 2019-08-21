import socket
import threading
import time

bind_ip = '0.0.0.0'
bind_port = 9000
file_path_name = '/home/raycad/tmp/raycad/test1.txt'
# Message loop times
message_loop_time = 200
# Time in seconds
seep_time = 0.1

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((bind_ip, bind_port))
server.listen(5)  # max backlog of connections

print('Listening on {}:{}'.format(bind_ip, bind_port))

def handle_client_connection(client_socket):
	try:
		with open(file_path_name, 'r') as content_file:
			content = content_file.read()
	
		#while True:
		for i in range(message_loop_time):
			client_socket.sendall(bytes(content + "\n", "utf-8"))			
			time.sleep(seep_time)
			
	except:
		print("Lost connection")

while True:
    client_sock, address = server.accept()
    print('Accepted connection from {}:{}'.format(address[0], address[1]))
    client_handler = threading.Thread(
        target=handle_client_connection,
        args=(client_sock,)  # without comma you'd get a... TypeError: handle_client_connection() argument after * must be a sequence, not _socketobject
    )
    client_handler.start()