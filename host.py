import socket
import subprocess
import time
import os
import argparse
import sys
import threading
import shutil
import http.server
import socketserver

# === Color Constants and Log Function ===
import os # Needed for os.system('') on Windows if required

# ANSI Color Codes
COLOR_RESET = '\033[0m'
COLOR_GREEN = '\033[92m' # Bright Green
COLOR_YELLOW = '\033[93m' # Bright Yellow
COLOR_CYAN = '\033[96m'  # Bright Cyan
COLOR_RED = '\033[91m'    # Bright Red

# --- Enable ANSI colors on Windows cmd.exe if needed ---
# (Modern Windows Terminal usually supports them by default)
if os.name == 'nt':
    os.system('')

# --- Logging ---
# Modified log function to accept color
def log(message, color=None):
    timestamp = time.strftime('%H:%M:%S')
    prefix = "[HOST]" # Host specific
    if color:
        print(f"{color}{prefix} {timestamp} - {message}{COLOR_RESET}", flush=True)
    else:
        print(f"{prefix} {timestamp} - {message}", flush=True)
# ==========================================

# --- Configuration ---
CHUNK_DIR = "host_chunks_hls" # Directory for HLS files
HTTP_PORT = 8000            # Port for HTTP server
CHUNK_DURATION_SECONDS = 4  # HLS segment duration (4-6s is common)
HOST_IP_FOR_INFO = '0.0.0.0' # IP to display (informational only)

# --- FFMPEG Function ---
def run_ffmpeg(input_video):
    """Starts the FFMPEG process to generate HLS stream."""
    if not os.path.exists(input_video):
         log(f"Error: Input video not found at {input_video}", color=COLOR_RED)
         sys.exit(1)

    # Ensure chunk directory exists and is clean
    if os.path.exists(CHUNK_DIR):
        log(f"Removing existing HLS chunk directory: {CHUNK_DIR}")
        try:
            shutil.rmtree(CHUNK_DIR)
        except OSError as e:
            log(f"Warning: Could not remove directory {CHUNK_DIR}: {e}", color=COLOR_YELLOW)
            # Attempt to continue if removal fails but dir exists
    try:
        os.makedirs(CHUNK_DIR, exist_ok=True)
        log(f"Created HLS chunk directory: {CHUNK_DIR}")
    except OSError as e:
        log(f"Error: Could not create directory {CHUNK_DIR}: {e}", color=COLOR_RED)
        sys.exit(1)


    # HLS specific filenames
    output_playlist = os.path.join(CHUNK_DIR, "playlist.m3u8")
    # Using simple segment names, ffmpeg adds sequence number automatically
    output_segment_pattern = os.path.join(CHUNK_DIR, "segment%05d.ts") # .ts is standard for HLS

    # Construct FFMPEG command for HLS
    ffmpeg_cmd = [
        'ffmpeg', '-hide_banner', # Less verbose ffmpeg output
        '-re',                  # Optional: Read input at native frame rate (simulate live)
        '-i', input_video,
        # Video/Audio encoding - COPY is fastest & preserves quality
        '-c:v', 'copy',
        '-c:a', 'copy',
        # Mapping: Select first video and first audio stream
        # Adjust if your desired streams are different (e.g., 0:a:1)
        '-map', '0:v:0',
        '-map', '0:a:0',
        # HLS Output Settings
        '-f', 'hls',                    # Format is HLS
        '-hls_time', str(CHUNK_DURATION_SECONDS), # Target duration of each segment
        '-hls_list_size', '5',          # Number of segments to keep in playlist (0=all, small number=more live feel)
        '-hls_flags', 'delete_segments', # Delete segments older than hls_list_size * hls_time
        '-hls_segment_filename', output_segment_pattern, # Naming pattern for segments
        output_playlist                 # Master playlist file
    ]

    log(f"Starting FFMPEG for HLS: {' '.join(ffmpeg_cmd)}")
    try:
        # Start FFMPEG, redirect output
        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        log(f"FFMPEG process started (PID: {process.pid}). Streaming to {output_playlist}")
        return process
    except FileNotFoundError:
        log("Error: 'ffmpeg' command not found. Is FFMPEG installed and in your PATH?", color=COLOR_RED)
        sys.exit(1)
    except Exception as e:
        log(f"Error starting FFMPEG: {e}", color=COLOR_RED)
        sys.exit(1)


# --- Simple HTTP Server Thread ---
httpd = None # Global variable to hold the server instance for shutdown
http_server_thread = None # Global variable for the thread itself

def start_http_server(port):
    """Starts a simple HTTP server in a background thread."""
    global httpd, http_server_thread

    # Handler that serves files from the current working directory
    # Important: Run host.py from the directory *above* where CHUNK_DIR will be created
    handler = http.server.SimpleHTTPRequestHandler
    socketserver.TCPServer.allow_reuse_address = True # Allow quick restarts

    try:
        httpd = socketserver.TCPServer(("", port), handler)
        log(f"HTTP Server serving on port {port} from directory '{os.getcwd()}'")
        # Start serving in the current thread (which is run by http_server_thread)
        httpd.serve_forever()
    except OSError as e:
         log(f"HTTP Server Error (Port {port} likely in use or permission issue): {e}", color=COLOR_RED)
         httpd = None # Ensure httpd is None if setup failed
    except Exception as e:
         log(f"HTTP Server failed unexpectedly: {e}", color=COLOR_RED)
         httpd = None
    log("HTTP server thread finished.")


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Host - Chunks video and serves HLS stream.")
    parser.add_argument("video_file", help="Path to the input video file.")
    parser.add_argument("-p", "--port", type=int, default=HTTP_PORT, help=f"Port for HTTP server (default: {HTTP_PORT}).")
    args = parser.parse_args()

    HTTP_PORT = args.port # Update port from arguments
    ffmpeg_proc = None # Initialize

    # Start HTTP server in a background thread
    log("Starting HTTP server thread...")
    http_server_thread = threading.Thread(target=start_http_server, args=(HTTP_PORT,), daemon=True)
    http_server_thread.start()
    time.sleep(1.0) # Give HTTP server a moment to bind the port

    # Check if HTTP server started successfully
    if httpd is None:
         log("HTTP Server failed to start. Exiting.", color=COLOR_RED)
         sys.exit(1)

    try:
        # *** Record FFMPEG start time ***
        start_ffmpeg_time = time.time()

        # Start FFMPEG process
        ffmpeg_proc = run_ffmpeg(args.video_file)

        # Main loop: Monitor FFMPEG process and wait for it to finish
        log("FFMPEG processing running. Host is serving HLS stream.")
        log(f"-> Participants can connect to: http://<HOST_IP>:{HTTP_PORT}/{CHUNK_DIR}/playlist.m3u8")
        log("(Replace <HOST_IP> with this machine's local IP address)")
        log("Press Ctrl+C to stop the host.")

        # Wait for FFMPEG to complete or handle errors
        # communicate() waits for process to terminate and reads stdout/stderr
        stderr_output = ffmpeg_proc.communicate()[1] # Wait and get stderr

        # *** Record FFMPEG end time and calculate duration ***
        end_ffmpeg_time = time.time()
        ffmpeg_duration = end_ffmpeg_time - start_ffmpeg_time
        log(f"FFMPEG process finished with exit code: {ffmpeg_proc.returncode}", color=COLOR_YELLOW)
        # *** Log the processing time ***
        log(f"Total FFMPEG processing/streaming time: {ffmpeg_duration:.2f} seconds", color=COLOR_CYAN)

        if ffmpeg_proc.returncode != 0:
             log("--- FFMPEG Error Output ---", color=COLOR_RED)
             try:
                 # Decode stderr, ignoring potential errors
                 log(stderr_output.decode(errors='ignore'), color=COLOR_RED)
             except Exception as e:
                 log(f"(Could not decode stderr: {e})", color=COLOR_RED)
             log("--- End FFMPEG Error Output ---", color=COLOR_RED)

        log("Host stream finished or FFMPEG terminated.")
        # Keep serving HTTP for a bit in case clients are finishing up
        log("Keeping HTTP server alive for 10 seconds...")
        time.sleep(10)


    except KeyboardInterrupt:
         log("KeyboardInterrupt received, shutting down.")
    except Exception as e:
        log(f"An unhandled exception occurred in main: {e}", color=COLOR_RED)
        import traceback
        traceback.print_exc()

    finally:
        log("Entering final cleanup...")

        # Terminate FFMPEG if it's still running (e.g., due to KeyboardInterrupt)
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
             log("Terminating lingering FFMPEG process...")
             ffmpeg_proc.terminate()
             try:
                 ffmpeg_proc.wait(timeout=2.0) # Wait for graceful termination
             except subprocess.TimeoutExpired:
                 log("FFMPEG did not terminate gracefully, killing.")
                 ffmpeg_proc.kill()
             except Exception as e:
                 log(f"Error terminating FFMPEG: {e}", color=COLOR_YELLOW)

        # Shutdown HTTP server
        if httpd:
            log("Shutting down HTTP server...")
            try:
                httpd.shutdown() # Signal serve_forever loop to stop
                httpd.server_close() # Close the socket
                log("HTTP server shut down.")
            except Exception as e:
                log(f"Error shutting down HTTP server: {e}", color=COLOR_YELLOW)

        # Wait for HTTP server thread to exit
        if http_server_thread and http_server_thread.is_alive():
             log("Waiting for HTTP server thread to exit...")
             http_server_thread.join(timeout=3.0)
             if http_server_thread.is_alive():
                  log("Warning: HTTP server thread did not exit cleanly.", color=COLOR_YELLOW)

        # Optional: Clean up the chunk directory
        # Consider adding a command-line flag to enable/disable cleanup
        # if os.path.exists(CHUNK_DIR):
        #     log(f"Cleaning up HLS chunk directory: {CHUNK_DIR}")
        #     try:
        #          shutil.rmtree(CHUNK_DIR)
        #     except Exception as e:
        #          log(f"Warning: Failed to clean up chunk directory {CHUNK_DIR}: {e}", color=COLOR_YELLOW)


        log("Host exit complete.")
