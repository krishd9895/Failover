#!/usr/bin/env python3
"""
###############################################################################
# PRODUCTION-READY AUTOMATIC HIGH-AVAILABILITY PROCESS WATCHDOG
#
# Configuration Rule:
# Only 'MONGO_URI' inside your .env file is strictly mandatory.
#
# All other settings are automatically calculated using fallback code logic
# if left blank or unmodified below.
###############################################################################
"""

# --- OPTIONAL MANUAL OVERRIDES ---
# Leave empty ("") to let the script auto-detect these based on your folder structure!
SERVICE_ID = ""       # Autodetects to '[folder_name]-[path_hash]' if left blank
START_COMMAND = ""    # Autodetects interpreter path + common targets if left blank

# --- Infrastructure & Database Configuration ---
DATABASE_NAME = "Failover"
COLLECTION_NAME = "Services"

# --- Cluster Timing Guardrails ---
HEARTBEAT_INTERVAL = 15
HEARTBEAT_TIMEOUT = 60      # Must be >= 3-4x interval to insulate against network transit jitter
CHECK_INTERVAL = 5
LOCAL_RETRY_LIMIT = 3
STARTUP_GRACE_PERIOD = 3    # Delay verification until process finishes internal startup hooks
MAX_NETWORK_GRACE_S = 30    # Continuous database blackout window allowed before stepping down

import os
import sys
import time
import shlex
import random
import socket
import signal
import hashlib
import platform
import logging
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymongo
from pymongo.errors import PyMongoError, ConnectionFailure, DuplicateKeyError

# ---------------------------------------------------------------------------
# 1. ENVIRONMENT & DETECTIVE IDENTITY INITIALIZATION
# ---------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
IS_WINDOWS = platform.system() == "Windows"

# 1. Mandatory validation check
if not MONGO_URI or not MONGO_URI.strip():
    print("CRITICAL CONFIGURATION ERROR: 'MONGO_URI' is missing from the environment (.env)!", file=sys.stderr)
    print("Please add 'MONGO_URI=your_mongodb_connection_string' to your .env file.", file=sys.stderr)
    sys.exit(1)

# 2. Get deterministic script directory location (resolving symlinks/junctions via realpath)
PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))
FOLDER_NAME = os.path.basename(PROJECT_PATH)
HOSTNAME = socket.gethostname()

# Generate an unalterable short hash of the absolute project location path
PATH_HASH = hashlib.sha1(PROJECT_PATH.encode('utf-8')).hexdigest()[:8]

# 3. Dynamic Smart Configuration Discovery Fallbacks
if not SERVICE_ID.strip():
    # Folder name combined with absolute path hash isolates separate clones on the same PC
    clean_folder = FOLDER_NAME.lower().replace('_', '-').replace(' ', '-')
    SERVICE_ID = f"{clean_folder}-{PATH_HASH}-service"

if not START_COMMAND.strip():
    # Detect common deployment script targets implicitly
    possible_entry_points = ["main.py", "bot.py", "app.py"]
    detected_target = None
    
    for filename in possible_entry_points:
        if os.path.exists(os.path.join(PROJECT_PATH, filename)):
            detected_target = filename
            break
            
    if detected_target:
        # Uses sys.executable to lock the execution context to the active venv/conda interpreter
        START_COMMAND = f'"{sys.executable}" "{detected_target}"'
    else:
        # Final fallback fallback if nothing matches
        START_COMMAND = f'"{sys.executable}" "main.py"'

# Unique identifier mapping this specific folder path instance on this server machine
NODE_ID = f"{HOSTNAME}:{PROJECT_PATH}"

# Detect runtime host platform environment purely for informational context logs
if os.getenv("PYCHARM_HOSTED"):
    IDE_CONTEXT = "PyCharm"
elif os.getenv("VSCODE_PID"):
    IDE_CONTEXT = "VS Code"
else:
    IDE_CONTEXT = "Terminal/Shell"

# Unique configuration hash matrix signature used to protect cluster alignment parity
CONFIG_PAYLOAD = f"{START_COMMAND.strip()}|{HEARTBEAT_INTERVAL}|{HEARTBEAT_TIMEOUT}"
CONFIG_FINGERPRINT = hashlib.sha256(CONFIG_PAYLOAD.encode('utf-8')).hexdigest()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node: %(node_id)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class NodeLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs["extra"] = {"node_id": NODE_ID}
        return msg, kwargs

logger = NodeLoggerAdapter(logging.getLogger("failover_watchdog"), {})

child_process = None
is_running = True
db_disconnect_tracker = None

# Open single long-lived, auto-pooling connection instance client
try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000, retryReads=True, retryWrites=True)
    db_collection = mongo_client[DATABASE_NAME][COLLECTION_NAME]
except Exception as init_err:
    print(f"CRITICAL: Failed to initialize PyMongo client structure pool: {init_err}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# 2. SYSTEM SIGNAL INTERCEPTION & PROCESS TREE LIFECYCLE
# ---------------------------------------------------------------------------
def handle_shutdown_signal(signum, frame):
    global is_running
    logger.info(f"Received termination signal ({signal.Signals(signum).name}). Cleaning local environment...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)

def terminate_child():
    """Wipes out the entire process tree cross-platform without orphans."""
    global child_process
    if child_process and child_process.poll() is None:
        logger.info("Terminating the managed application process tree...")
        if IS_WINDOWS:
            try:
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(child_process.pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            except Exception as e:
                logger.error(f"Windows taskkill tree termination failed: {e}")
        else:
            try:
                os.killpg(os.getpgid(child_process.pid), signal.SIGTERM)
                for _ in range(10):
                    if child_process.poll() is not None:
                        break
                    time.sleep(1)
                else:
                    logger.warning("Application resisted SIGTERM. Issuing SIGKILL to process group...")
                    os.killpg(os.getpgid(child_process.pid), signal.SIGKILL)
                    child_process.wait()
            except Exception as e:
                logger.error(f"POSIX group tree termination failed: {e}")
    child_process = None

# ---------------------------------------------------------------------------
# 3. TRANSITIONAL LEADER ELECTORATE
# ---------------------------------------------------------------------------
def setup_database_indexes():
    try:
        db_collection.create_index([("owner_node_id", pymongo.ASCENDING)], background=True)
        return True
    except PyMongoError as e:
        logger.error(f"Failed to optimize indexing configuration layouts: {e}")
        return False

def bootstrap_and_validate_lock():
    """Idempotently handles baseline cluster collection document verification."""
    try:
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            initial_state = {
                "_id": SERVICE_ID,
                "owner_node_id": None,
                "status": "offline",
                "last_heartbeat": datetime.fromtimestamp(0, tz=timezone.utc),
                "config_fingerprint": CONFIG_FINGERPRINT
            }
            try:
                db_collection.insert_one(initial_state)
                logger.info("Successfully bootstrapped the missing cluster control record.")
            except DuplicateKeyError:
                pass 
            return True

        if doc.get("config_fingerprint") != CONFIG_FINGERPRINT:
            logger.critical(
                f"🚨 CONFIGURATION FINGERPRINT MISMATCH!\n"
                f"Another server node registered SERVICE_ID '{SERVICE_ID}' using different timing parameters or script calls.\n"
                f"Execution immediately halted to prevent cluster split-brain collisions."
            )
            sys.exit(1)
            
        return True
    except (ConnectionFailure, PyMongoError) as e:
        logger.error(f"Error checking cluster validation status: {e}. Retrying pool in 5s...")
        time.sleep(5)
        return False

def release_leadership():
    """Removes lease details cleanly using server-side time primitives."""
    try:
        query = {"_id": SERVICE_ID, "owner_node_id": NODE_ID}
        update = {
            "$set": {
                "owner_node_id": None,
                "status": "offline",
                "last_heartbeat": datetime.fromtimestamp(0, tz=timezone.utc)
            }
        }
        db_collection.update_one(query, update)
        logger.info("Released leadership lock in cluster collection successfully.")
    except Exception as e:
        logger.error(f"Failed to issue clean leadership release: {e}")

def try_acquire_or_maintain_leadership(force_check_only=False):
    """Acquires or maintains leadership using remote server time filters."""
    global db_disconnect_tracker

    try:
        if force_check_only:
            doc = db_collection.find_one({"_id": SERVICE_ID})
            db_disconnect_tracker = None 
            return doc and doc.get("owner_node_id") == NODE_ID

        filter_query = {
            "_id": SERVICE_ID,
            "$expr": {
                "$or": [
                    {"$eq": ["$owner_node_id", NODE_ID]},
                    {"$eq": ["$owner_node_id", None]},
                    {"$gt": [
                        "$$NOW", 
                        {"$add": ["$last_heartbeat", HEARTBEAT_TIMEOUT * 1000]}
                    ]}
                ]
            }
        }

        update_modifier = {
            "$set": {
                "owner_node_id": NODE_ID,
                "status": "active",
                "config_fingerprint": CONFIG_FINGERPRINT,
                "project_path": PROJECT_PATH,
                "python_interpreter": sys.executable,
                "runtime_context": IDE_CONTEXT
            },
            "$currentDate": {
                "last_heartbeat": True
            }
        }

        result = db_collection.find_one_and_update(
            filter_query, update_modifier, upsert=True, return_document=pymongo.ReturnDocument.AFTER
        )
        db_disconnect_tracker = None 
        return result and result.get("owner_node_id") == NODE_ID

    except (ConnectionFailure, PyMongoError) as e:
        logger.error(f"Database network communication fault: {e}")
        if db_disconnect_tracker is None:
            db_disconnect_tracker = time.time()
            
        if (time.time() - db_disconnect_tracker) > MAX_NETWORK_GRACE_S:
            logger.critical(f"🚨 CIRCUIT BREAKER TRIPPED: DB offline >{MAX_NETWORK_GRACE_S}s. Dropping lease.")
            return False 
            
        return True

# ---------------------------------------------------------------------------
# 4. RUNTIME MAIN LOOP
# ---------------------------------------------------------------------------
def main():
    global child_process, is_running
    
    print(f"======================================================================", flush=True)
    print(f"🔥 HA WATCHDOG ACTIVE | Engine: v{platform.python_version()}", flush=True)
    print(f"Service ID : {SERVICE_ID}", flush=True)
    print(f"Node ID    : {NODE_ID} ({IDE_CONTEXT})", flush=True)
    print(f"Interpreter: {sys.executable}", flush=True)
    print(f"Command    : {START_COMMAND}", flush=True)
    print(f"======================================================================\n", flush=True)
    
    time.sleep(random.uniform(0.5, 3.5))

    if not setup_database_indexes() or not bootstrap_and_validate_lock():
        return

    is_leader = False
    local_failures = 0
    last_heartbeat_time = 0
    cmd_args = shlex.split(START_COMMAND)

    while is_running:
        try:
            # --- STANDBY MONITORING LAYER ---
            if not is_leader:
                if not try_acquire_or_maintain_leadership():
                    time.sleep(CHECK_INTERVAL)
                    continue
                
                if not bootstrap_and_validate_lock():
                    release_leadership()
                    time.sleep(CHECK_INTERVAL)
                    continue
                    
                logger.info("🎉 SUCCESS: Captured distributed leadership! Transitioning to ACTIVE.")
                is_leader = True
                local_failures = 0

            # --- ACTIVE SUPERVISOR LAYER ---
            if is_leader:
                if child_process is None or child_process.poll() is not None:
                    if child_process and child_process.poll() is not None:
                        exit_code = child_process.poll()
                        local_failures += 1
                        logger.warning(f"Application crash caught (Code: {exit_code}). Failures: {local_failures}/{LOCAL_RETRY_LIMIT}")
                        child_process = None

                        if local_failures > LOCAL_RETRY_LIMIT:
                            logger.critical("Local recovery limit breached. Relinquishing leadership lock.")
                            release_leadership()
                            is_leader = False
                            time.sleep(CHECK_INTERVAL)
                            continue

                    if not try_acquire_or_maintain_leadership():
                        logger.warning("Split-brain caught during crash recovery phase. Reverting to standby.")
                        is_leader = False
                        continue

                    logger.info(f"Executing application: {cmd_args}")
                    try:
                        if IS_WINDOWS:
                            child_process = subprocess.Popen(
                                cmd_args, 
                                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                            )
                        else:
                            child_process = subprocess.Popen(
                                cmd_args, 
                                start_new_session=True
                            )
                            
                        time.sleep(STARTUP_GRACE_PERIOD)
                        
                        if child_process.poll() is not None:
                            logger.error("Application died within the startup grace window.")
                            continue

                        if try_acquire_or_maintain_leadership():
                            last_heartbeat_time = time.time()
                            logger.info("Application passed initial checks. Heartbeat tracking active.")
                        else:
                            logger.critical("Failed to retain lock during verification. Stopping application.")
                            terminate_child()
                            is_leader = False
                            continue
                    except Exception as e:
                        logger.error(f"System failure attempting to initiate process target: {e}")
                        child_process = None
                        time.sleep(CHECK_INTERVAL)
                        continue

                # --- STEADY-STATE RUNTIME OPERATION ---
                current_time = time.time()
                
                if not try_acquire_or_maintain_leadership(force_check_only=True):
                    logger.critical("🚨 STALE OWNER DETECTED: Node identity overtaken by cluster! Stopping local application.")
                    terminate_child()
                    is_leader = False
                    continue

                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    if child_process.poll() is None:
                        if try_acquire_or_maintain_leadership():
                            logger.info("Heartbeat logged successfully via remote server clock.")
                            last_heartbeat_time = current_time
                            local_failures = 0 
                        else:
                            logger.critical("🚨 LEASE LOST: Lock overridden during heartbeat update! Stopping application.")
                            terminate_child()
                            is_leader = False
                    else:
                        logger.warning("Supervised process dead inside the scheduled pulse window.")

                time.sleep(1)

        except PyMongoError as e:
            logger.error(f"Database infrastructure connectivity issue: {e}. Re-verifying pool...")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Unhandled exception in runtime supervisor loop: {e}")
            time.sleep(2)

    terminate_child()
    if is_leader:
        release_leadership()
    logger.info("Watchdog cleanup executed cleanly. Shutting down wrapper.")

if __name__ == "__main__":
    main()
    
