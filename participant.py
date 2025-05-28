import subprocess
import argparse
import time
import sys
import requests # To check if stream is available
import os      # For os.name and os.system

# === Color Constants and Log Function (Same as before) ===
# ANSI Color Codes
COLOR_RESET = '\033[0m'
COLOR_GREEN = '\033[92m' # Bright Green
COLOR_YELLOW = '\033[93m' # Bright Yellow
COLOR_CYAN = '\033[96m'  # Bright Cyan
COLOR_RED = '\033[91m'    # Bright Red
# --- Enable ANSI colors on Windows cmd.exe if needed ---
if os.name == 'nt':
    os.system('')
# --- Logging ---
def log(message, color=None):
    timestamp = time.strftime('%H:%M:%S')
    prefix = "[PARTICIPANT]" # Participant specific
    if color:
        print(f"{color}{prefix} {timestamp} - {message}{COLOR_RESET}", flush=True)
    else:
        print(f"{prefix} {timestamp} - {message}", flush=True)
# ======================================================

# --- Player Configuration ---
#PLAYER_COMMAND = ['vlc', '--play-and-exit']
#PLAYER_COMMAND = ['mpv']
PLAYER_COMMAND = ['ffplay', '-autoexit']

# --- Function to check stream availability ---
def wait_for_stream(url, timeout=30):
    # ... (Function is the same as before) ...
    log(f"Waiting for stream playlist to become available at: {url}")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.head(url, timeout=3) # Slightly longer timeout for internet
            if response.status_code == 200:
                log("Stream playlist found!", color=COLOR_GREEN)
                return True
            # Handle potential redirects if ngrok uses them (unlikely for direct tunnel)
            elif 300 <= response.status_code < 400:
                 log(f"Received redirect status {response.status_code}, checking new location if available...", color=COLOR_YELLOW)
                 # Basic redirect handling might be needed in complex setups, ignore for now
            else:
                 log(f"Received status {response.status_code}, waiting...")
        except requests.exceptions.ConnectionError:
            # log("Connection error, host/tunnel might not be up yet. Retrying...") # Reduce noise
            pass
        except requests.exceptions.Timeout:
            log("Request timed out. Retrying...", color=COLOR_YELLOW)
        except Exception as e:
            log(f"Error checking stream URL: {e}", color=COLOR_YELLOW)
        time.sleep(3) # Slightly longer sleep for internet checks
    log(f"Error: Stream playlist not found within {timeout} seconds.", color=COLOR_RED)
    return False


# --- Main Execution ---
if __name__ == "__main__":
    script_start_time = time.time()

    # *** MODIFIED ARGUMENT PARSING ***
    parser = argparse.ArgumentParser(description="Participant - Plays HLS stream from host via IP or ngrok URL.")
    parser.add_argument("target", help="Host's local IP address OR the full ngrok base URL (e.g., https://random.ngrok-free.app)")
    parser.add_argument("-p", "--port", type=int, default=8000, help="HTTP port host is serving on (used only if target is an IP address, default: 8000).")
    args = parser.parse_args()
    # *** END MODIFIED ARGUMENT PARSING ***


    playlist_filename = "playlist.m3u8"
    chunk_dir_name = "host_chunks_hls" # Should match CHUNK_DIR in host.py

    # *** CONSTRUCT URL BASED ON INPUT TYPE ***
    if args.target.startswith("http://") or args.target.startswith("https://"):
        # Assume target is a full ngrok base URL
        base_url = args.target.rstrip('/') # Remove trailing slash if present
        playlist_url = f"{base_url}/{chunk_dir_name}/{playlist_filename}"
        log("Received ngrok/http base URL.")
    else:
        # Assume target is an IP address, construct URL with port
        base_url = f"http://{args.target}:{args.port}"
        playlist_url = f"{base_url}/{chunk_dir_name}/{playlist_filename}"
        log("Received IP address, constructing local URL.")
    # *** END URL CONSTRUCTION ***


    log(f"Target HLS stream URL: {playlist_url}")

    player_process = None

    try:
        # Wait slightly longer for internet connection
        if not wait_for_stream(playlist_url, timeout=90): # Wait up to 90 seconds
            log("Exiting because stream could not be found.", color=COLOR_RED)
            sys.exit(1)

        player_launch_time = time.time()
        time_to_playback_start = player_launch_time - script_start_time
        log(f"Time from script start until player launch: {time_to_playback_start:.2f} seconds", color=COLOR_GREEN)

        cmd = PLAYER_COMMAND + [playlist_url]
        log(f"Starting player: {' '.join(cmd)}")

        player_process = subprocess.Popen(cmd)
        player_process.wait()

        exit_code = player_process.returncode
        log(f"Player process finished with exit code: {exit_code}", color=COLOR_YELLOW if exit_code != 0 else None)

    # ... (rest of except/finally blocks are the same as before) ...
    except FileNotFoundError:
        log(f"Error: Player command not found ('{PLAYER_COMMAND[0]}').", color=COLOR_RED)
        log("Please install a compatible player (VLC, mpv, or ensure ffplay is in PATH) and configure PLAYER_COMMAND.", color=COLOR_YELLOW)
    except KeyboardInterrupt:
        log("KeyboardInterrupt received, stopping participant.")
    except Exception as e:
        log(f"An error occurred: {e}", color=COLOR_RED)
        import traceback
        traceback.print_exc()
    finally:
        if player_process and player_process.poll() is None:
            log("Terminating player process...")
            player_process.terminate()
            try: player_process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                log("Player did not terminate gracefully, killing.")
                player_process.kill()
            except Exception as e: log(f"Error terminating player: {e}", color=COLOR_YELLOW)
        log("Participant finished.")
