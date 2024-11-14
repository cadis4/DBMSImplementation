# server_handler.py
import socket
from server_commands import process_command

def handle_client(conn, addr):
    print(f"Connection from {addr} has been established.")
    conn.sendall("Welcome to the Mini DBMS server!".encode('utf-8'))
    
    while True:
        try:
            command = conn.recv(1024).decode('utf-8')
            if not command:
                break
            print(f"Received command: {command}")
            response = process_command(command)
            conn.sendall(response.encode('utf-8'))
        except Exception as e:
            print(f"Error: {e}")
            break
    
    conn.close()
    print(f"Connection from {addr} has been closed.")
