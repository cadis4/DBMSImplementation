import socket

HOST = '127.0.0.1'  # Server IP address
PORT = 65431       # Server port

# Function to start the client
def start_client():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    
    print(client.recv(1024).decode('utf-8'))  # Welcome message from server

    while True:
        command = input("Enter command: ")
        if command.lower() == 'exit':
            print("Closing connection...")
            break
        
        client.send(command.encode('utf-8'))
        response = client.recv(1024).decode('utf-8')
        print(response)
    
    client.close()

if __name__ == "__main__":
    start_client()
