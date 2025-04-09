import socket
import time
import os
import argparse
import struct

# --- Configuration ---
RECEIVE_DIR = "participant_received_chunks"

# --- Logging ---
def log(message):
    print(f"[PARTICIPANT] {time.strftime('%H:%M:%S')} - {message}", flush=True)

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Participant - Receives video chunks from the host.")
    parser.add_argument("host_ip", help="IP address of the host.")
    parser.add_argument("-p", "--port", type=int, default=65432, help="Port the host is listening on.")
    args = parser.parse_args()

    HOST = args.host_ip
    PORT = args.port

    if os.path.exists(RECEIVE_DIR):
        log(f"Removing existing receive directory: {RECEIVE_DIR}")
        import shutil
        shutil.rmtree(RECEIVE_DIR)
    os.makedirs(RECEIVE_DIR, exist_ok=True)
    log(f"Created receive directory: {RECEIVE_DIR}")

    total_bytes_received = 0
    start_time = time.time()

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        log(f"Connecting to host {HOST}:{PORT}...")
        client_socket.connect((HOST, PORT))
        log("Connected successfully.")

        while True:
            # Receive header: [4-byte index][4-byte filename_len][filename][8-byte data_len]
            header_part1 = client_socket.recv(4 + 4) # Index + filename_len
            if not header_part1 or len(header_part1) < 8:
                log("Connection closed by host or incomplete header part 1.")
                break

            chunk_index = int.from_bytes(header_part1[0:4], 'big')
            filename_len = int.from_bytes(header_part1[4:8], 'big')

            # Receive filename
            filename_bytes = client_socket.recv(filename_len)
            if not filename_bytes or len(filename_bytes) < filename_len:
                 log("Connection closed by host or incomplete filename.")
                 break
            filename = filename_bytes.decode('utf-8')

             # Receive data length
            data_len_bytes = client_socket.recv(8)
            if not data_len_bytes or len(data_len_bytes) < 8:
                 log("Connection closed by host or incomplete data length.")
                 break
            data_len = int.from_bytes(data_len_bytes, 'big')

            log(f"Receiving chunk {chunk_index}: {filename}, expecting {data_len} bytes...")

            # Receive data
            chunk_data = b''
            bytes_received = 0
            while bytes_received < data_len:
                bytes_to_recv = min(4096, data_len - bytes_received)
                packet = client_socket.recv(bytes_to_recv)
                if not packet:
                    log("Connection closed prematurely by host during data transfer.")
                    raise ConnectionAbortedError("Host disconnected")
                chunk_data += packet
                bytes_received += len(packet)

            if bytes_received != data_len:
                 log(f"Error: Received {bytes_received} bytes but expected {data_len} for {filename}")
                 # Decide how to handle - skip file, abort, etc.
                 continue # Skip saving this potentially corrupted chunk

            save_path = os.path.join(RECEIVE_DIR, filename)
            with open(save_path, 'wb') as f:
                f.write(chunk_data)

            actual_size = len(chunk_data)
            total_bytes_received += actual_size
            log(f"Successfully received and saved chunk {chunk_index}: {filename} (Size: {actual_size} bytes)")


    except ConnectionRefusedError:
        log(f"Error: Connection refused. Is the host running on {HOST}:{PORT}?")
    except ConnectionAbortedError as e:
        log(f"Error: {e}")
    except Exception as e:
        log(f"An error occurred: {e}")
    finally:
        if 'client_socket' in locals() and client_socket:
            client_socket.close()
            log("Socket closed.")

        end_time = time.time()
        duration = end_time - start_time
        log(f"\n--- Summary ---")
        log(f"Total time: {duration:.2f} seconds")
        log(f"Total data received: {total_bytes_received} bytes")
        if duration > 0:
             rate_kbps = (total_bytes_received * 8) / (duration * 1024)
             log(f"Average transfer rate: {rate_kbps:.2f} Kbps")
        log(f"Chunks saved in: {RECEIVE_DIR}")
        log("Participant finished.")