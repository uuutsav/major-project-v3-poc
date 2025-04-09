import socket
import subprocess
import time
import os
import argparse
import sys
import threading
import shutil

# --- Configuration ---
CHUNK_DIR = "host_chunks"
CHUNK_PREFIX = "chunk_"
CHUNK_FORMAT = "mp4"
CHUNK_DURATION_SECONDS = 5
HOST = '0.0.0.0'
PORT = 65432

# --- Logging ---
def log(message):
    print(f"[HOST] {time.strftime('%H:%M:%S')} - {message}", flush=True)

# --- Shared State ---
clients = []
clients_lock = threading.Lock()
server_running = True

# *** NEW: List to store paths of available chunks ***
available_chunk_paths = []
available_paths_lock = threading.Lock() # Lock for accessing the list

# Global State for Monitoring
last_checked_files = set()

# *** MODIFIED: Send a specific chunk to a specific connection ***
def send_single_chunk(conn, chunk_path, chunk_index):
    """Sends one chunk file over a given socket connection."""
    if not os.path.exists(chunk_path):
        log(f"Error: Chunk {chunk_path} not found for sending to specific client.")
        return False
    try:
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
        chunk_size = len(chunk_data)
        if chunk_size == 0:
             #log(f"Skipping empty chunk {chunk_index} for specific client.")
             return True # Consider success if empty

        filename = os.path.basename(chunk_path)
        #log(f"Sending existing chunk {chunk_index} ({filename}, {chunk_size} bytes) to {conn.getpeername()}")

        header = chunk_index.to_bytes(4, 'big') + \
                 len(filename).to_bytes(4, 'big') + \
                 filename.encode('utf-8') + \
                 chunk_size.to_bytes(8, 'big')

        data_to_send = header + chunk_data
        conn.sendall(data_to_send)
        #log(f"Finished sending existing chunk {chunk_index} to {conn.getpeername()}")
        return True

    except FileNotFoundError:
         log(f"Error: Chunk file {chunk_path} disappeared before sending to specific client.")
         return False
    except (ConnectionResetError, BrokenPipeError, socket.timeout):
         log(f"Client {conn.getpeername()} disconnected or timed out during specific chunk send.")
         return False
    except Exception as e:
        log(f"Error sending chunk {chunk_path} to specific client {conn.getpeername()}: {e}")
        return False

# *** MODIFIED handle_client function ***
def handle_client(conn, addr):
    log(f"Participant connected: {addr}")

    # 1. Send existing chunks first
    paths_to_send_on_connect = []
    with available_paths_lock:
        # Make a copy to avoid holding lock during potentially long send loop
        paths_to_send_on_connect = sorted(
            list(available_chunk_paths),
            key=lambda p: int(os.path.basename(p).replace(CHUNK_PREFIX, "").replace(f".{CHUNK_FORMAT}", ""))
        )

    log(f"Preparing to send {len(paths_to_send_on_connect)} existing chunks to new participant {addr}...")
    client_still_connected = True
    for chunk_path in paths_to_send_on_connect:
        try:
            chunk_filename = os.path.basename(chunk_path)
            chunk_index = int(chunk_filename.replace(CHUNK_PREFIX, "").replace(f".{CHUNK_FORMAT}", ""))
            if not send_single_chunk(conn, chunk_path, chunk_index):
                client_still_connected = False
                break # Stop sending if an error occurs
        except ValueError:
            log(f"Could not parse index from existing chunk path: {chunk_path}")
        except Exception as e:
            log(f"Unexpected error sending existing chunk {chunk_path} to {addr}: {e}")
            client_still_connected = False
            break

    if client_still_connected:
        log(f"Finished sending existing chunks to {addr}.")
    else:
        log(f"Stopped sending existing chunks to {addr} due to error or disconnect.")

    # 2. Add to list for future chunks (if still connected)
    if client_still_connected:
        with clients_lock:
            clients.append(conn)
        log(f"Participant {addr} added to receive future chunks.")
    else:
         # If sending initial chunks failed, close connection immediately
         log(f"Participant {addr} failed initial chunk sync, closing connection.")
         try:
              conn.close()
         except Exception: pass
         return # End the handler thread for this client

    # 3. Keep connection alive (or detect disconnects) while waiting for future chunks
    try:
        conn.settimeout(5.0)
        while server_running:
            try:
                data = conn.recv(1, socket.MSG_PEEK)
                if not data:
                    log(f"Participant {addr} appears disconnected (recv returned empty).")
                    break
                time.sleep(2) # Keepalive check interval
            except socket.timeout:
                 continue # Timeout is normal, just means no data from client
            except (ConnectionResetError, BrokenPipeError):
                 log(f"Participant {addr} disconnected abruptly while idle.")
                 break
            except Exception as e:
                 log(f"Error checking participant {addr} connection: {e}")
                 break

    except Exception as e:
        log(f"Error in handle_client main loop for {addr}: {e}")
    finally:
        log(f"Participant {addr} cleaning up connection.")
        with clients_lock:
            if conn in clients:
                clients.remove(conn)
        try:
             # Check if socket is already closed before trying to close again
             if conn.fileno() != -1:
                 conn.close()
        except Exception as e:
             # log(f"Info: Error closing socket for {addr} (might be already closed): {e}")
             pass


def start_server_loop(listening_socket):
    global server_running
    while server_running:
        try:
             # log("Server waiting for new connection...")
             conn, addr = listening_socket.accept()
             # Start a new thread for each client to handle initial sync + future chunks
             client_thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
             client_thread.start()
        except socket.timeout:
             continue
        except OSError as e:
             if server_running and e.errno != 9: # 9: Bad file descriptor (expected on close)
                  log(f"Server accept error: {e}")
             else:
                  pass # Socket closed normally
             break
        except Exception as e:
            log(f"Unexpected error in server accept loop: {e}")
            break


# *** MODIFIED: Now sends chunk to *currently connected* clients ***
def send_chunk_to_all_current(chunk_path, chunk_index):
    """Sends chunk data to all clients currently in the global list."""
    disconnected_clients = []
    sent_to_any = False
    with clients_lock:
        if not clients:
             # log(f"No participants connected for chunk {chunk_index}")
             return # Nothing to do

        # Use a copy of the list in case of modifications during iteration (less likely now)
        current_client_list = list(clients)

    #log(f"Sending new chunk {chunk_index} to {len(current_client_list)} participant(s)...")
    for conn in current_client_list:
        if send_single_chunk(conn, chunk_path, chunk_index):
            sent_to_any = True
        else:
            # If send failed, mark client for removal
            disconnected_clients.append(conn)
            log(f"Marking client {conn.getpeername()} for removal after failed send.")


    # Remove disconnected clients after iteration
    if disconnected_clients:
        with clients_lock:
            for conn in disconnected_clients:
                if conn in clients:
                    clients.remove(conn)
                try:
                    # Ensure socket is closed if send failed
                    if conn.fileno() != -1:
                         conn.close()
                except Exception: pass # Ignore errors during cleanup close

    # No need to log success here, send_single_chunk does enough logging


# --- FFMPEG & Monitoring ---
def run_ffmpeg(input_video):
    # ... (run_ffmpeg function remains the same) ...
    if not os.path.exists(input_video):
         log(f"Error: Input video not found at {input_video}")
         sys.exit(1)

    if os.path.exists(CHUNK_DIR):
        log(f"Removing existing chunk directory: {CHUNK_DIR}")
        shutil.rmtree(CHUNK_DIR)
    os.makedirs(CHUNK_DIR, exist_ok=True)
    log(f"Created chunk directory: {CHUNK_DIR}")

    output_pattern = os.path.join(CHUNK_DIR, f"{CHUNK_PREFIX}%03d.{CHUNK_FORMAT}")

    ffmpeg_cmd = [
        'ffmpeg', '-hide_banner',
        '-i', input_video,
        '-c', 'copy',
        '-map', '0:v:0',
        '-map', '0:a:0',
        '-segment_time', str(CHUNK_DURATION_SECONDS),
        '-f', 'segment',
        '-reset_timestamps', '1',
        '-segment_format', CHUNK_FORMAT,
        '-segment_list_flags', '+live',
        '-break_non_keyframes', '1',
        output_pattern
    ]

    log(f"Starting FFMPEG: {' '.join(ffmpeg_cmd)}")
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    return process

def monitor_and_send(ffmpeg_process):
    processed_chunk_indices = set() # Track sent indices
    stable_files = {} # Track files potentially stable {filename: size}

    log("Starting monitoring loop...")
    while True:
        ffmpeg_status = ffmpeg_process.poll()
        if ffmpeg_status is not None:
            log(f"FFMPEG process finished with code: {ffmpeg_status}")
            time.sleep(0.5)
            check_for_new_chunks(processed_chunk_indices, stable_files, final_check=True)
            if ffmpeg_status != 0:
                 stderr_output = ffmpeg_process.stderr.read().decode(errors='ignore')
                 log(f"FFMPEG Error Output:\n{stderr_output}")
            break

        check_for_new_chunks(processed_chunk_indices, stable_files)
        time.sleep(0.2)

    log("Monitoring loop finished.")


def check_for_new_chunks(processed_chunk_indices, stable_files, final_check=False):
    global last_checked_files # Declare global at the start

    try:
        current_files = set(os.listdir(CHUNK_DIR))
        newly_detected_files = current_files - last_checked_files

        # --- Check stability of previously detected files ---
        files_to_process = [] # Files deemed stable this cycle
        files_to_remove_from_stable = []
        for filename, last_size in list(stable_files.items()):
             if filename not in current_files:
                  files_to_remove_from_stable.append(filename)
                  continue

             chunk_path = os.path.join(CHUNK_DIR, filename)
             try:
                 current_size = os.path.getsize(chunk_path)
                 if current_size == last_size and current_size > 0:
                      if filename.startswith(CHUNK_PREFIX) and filename.endswith(f".{CHUNK_FORMAT}"):
                           files_to_process.append(filename)
                      files_to_remove_from_stable.append(filename)
                 elif current_size != last_size:
                      stable_files[filename] = current_size
             except FileNotFoundError:
                  files_to_remove_from_stable.append(filename)
             except Exception as e:
                  log(f"Error checking size for {filename}: {e}")
                  files_to_remove_from_stable.append(filename)

        for filename in files_to_remove_from_stable:
             if filename in stable_files:
                 del stable_files[filename]

        # --- Process newly detected files ---
        for filename in newly_detected_files:
             if filename.startswith(CHUNK_PREFIX) and filename.endswith(f".{CHUNK_FORMAT}"):
                 chunk_path = os.path.join(CHUNK_DIR, filename)
                 try:
                     stable_files[filename] = os.path.getsize(chunk_path)
                 except FileNotFoundError: pass
                 except Exception as e: log(f"Error getting initial size for {filename}: {e}")

        # --- Final check processing ---
        if final_check:
             final_files = set()
             try: final_files = set(f for f in os.listdir(CHUNK_DIR) if f.startswith(CHUNK_PREFIX) and f.endswith(f".{CHUNK_FORMAT}"))
             except FileNotFoundError: pass

             for filename in final_files:
                   if filename not in files_to_process: # Only add if not already stable
                        # Requires parsing index to check processed_chunk_indices
                        try:
                            chunk_idx = int(filename.replace(CHUNK_PREFIX, "").replace(f".{CHUNK_FORMAT}", ""))
                            if chunk_idx not in processed_chunk_indices:
                                chunk_path = os.path.join(CHUNK_DIR, filename)
                                if os.path.exists(chunk_path) and os.path.getsize(chunk_path) > 0:
                                    files_to_process.append(filename)
                                elif filename in stable_files: del stable_files[filename]
                        except ValueError: pass
                        except Exception as e: log(f"Error processing final file {filename}: {e}")


        # --- Sort and Process Stable Files ---
        files_to_process.sort(key=lambda f: int(f.replace(CHUNK_PREFIX, "").replace(f".{CHUNK_FORMAT}", "")))

        for filename in files_to_process:
            try:
                chunk_index = int(filename.replace(CHUNK_PREFIX, "").replace(f".{CHUNK_FORMAT}", ""))
                if chunk_index not in processed_chunk_indices:
                    chunk_path = os.path.join(CHUNK_DIR, filename)

                    # *** ADD to available list ***
                    with available_paths_lock:
                         if chunk_path not in available_chunk_paths: # Avoid duplicates if somehow processed twice
                              available_chunk_paths.append(chunk_path)

                    # *** SEND to current clients ***
                    send_chunk_to_all_current(chunk_path, chunk_index)

                    processed_chunk_indices.add(chunk_index) # Mark index as processed
            except ValueError: log(f"Could not parse chunk index from filename: {filename}")
            except Exception as e: log(f"Error during processing/sending stable file {filename}: {e}")

        # Update last checked files
        last_checked_files = current_files

    except FileNotFoundError:
         if final_check: log("Chunk directory not found during final check.")
         pass
    except Exception as e:
         log(f"Error listing/processing chunks: {e}")


# --- Main Execution ---
server_thread = None
server_socket_global = None

def cleanup_server():
    # ... (cleanup_server function remains mostly the same) ...
    global server_running, server_socket_global
    log("Initiating server cleanup...")
    server_running = False

    if server_socket_global:
        log("Closing listening server socket...")
        try:
            server_socket_global.close()
            server_socket_global = None
            log("Server socket closed.")
        except Exception as e: log(f"Error closing server socket: {e}")

    log("Closing active client connections...")
    with clients_lock:
        client_list = list(clients)
        clients.clear()
    for conn in client_list:
        try:
            peer = conn.getpeername() # Get peername before potential close
            log(f"Shutting down connection {peer}...")
            conn.shutdown(socket.SHUT_RDWR)
        except (OSError, socket.error): pass
        finally:
            try:
                conn.close()
            except Exception: pass
    log("Client connections closed.")


if __name__ == "__main__":
    last_checked_files = set() # Initialize global
    available_chunk_paths = [] # Initialize global list

    parser = argparse.ArgumentParser(description="Host - Chunks video and sends pieces to participants.")
    # ... (rest of __main__ remains the same) ...
    parser.add_argument("video_file", help="Path to the input video file.")
    parser.add_argument("-p", "--port", type=int, default=PORT, help="Port to listen on.")
    args = parser.parse_args()

    PORT = args.port
    ffmpeg_proc = None

    try:
        server_socket_global = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket_global.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket_global.bind((HOST, PORT))
        server_socket_global.listen(5)
        server_socket_global.settimeout(1.0)
        log(f"TCP Server listening on {HOST}:{PORT}")

        server_thread = threading.Thread(target=start_server_loop, args=(server_socket_global,), daemon=True)
        server_thread.start()

        ffmpeg_proc = run_ffmpeg(args.video_file)
        monitor_and_send(ffmpeg_proc) # This blocks until ffmpeg finishes

        log("FFMPEG processing finished. Waiting for remaining sends/clients...")
        # Keep main thread alive briefly to allow server thread to handle connections/sends
        # Or implement a more robust shutdown signal
        time.sleep(5) # Wait a bit for final sends? Might need better logic

        log("Host process attempting to finish.")

    except KeyboardInterrupt:
         log("KeyboardInterrupt received, shutting down.")
    except Exception as e:
        log(f"Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        log("Entering final cleanup...")
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
             log("Terminating lingering ffmpeg process...")
             ffmpeg_proc.terminate()
             try: ffmpeg_proc.wait(timeout=2.0)
             except subprocess.TimeoutExpired: ffmpeg_proc.kill()

        cleanup_server()

        if server_thread and server_thread.is_alive():
             log("Waiting for server thread to exit...")
             server_thread.join(timeout=3.0)
             if server_thread.is_alive():
                  log("Warning: Server thread did not exit cleanly.")

        log("Host exit complete.")