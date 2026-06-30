#!/usr/bin/env python3
"""
###############################################################################
# PRODUCTION-GRADE HIGH-AVAILABILITY MULTI-MODE PROCESS SUPERVISOR
#
# INSTRUCTIONS:
# 1. Provide your 'MONGO_URI' inside your local .env file (Mandatory).
# 2. Select your 'LAUNCH_MODE' below: "PYTHON", "DOCKER_COMPOSE", or "RAW".
# 3. Define your target script or parameters inside the 'TARGET' parameter.
#
# ADMINISTRATIVE OVERRIDE MANUAL:
# To force an instant takeover, update your MongoDB collection document:
# db.Services.updateOne({"_id": "YOUR_SERVICE_ID"}, {"$set": {"forced_leader_node": "NODE_ALIAS"}})
# (Find your explicit NODE_ALIAS printed directly inside the startup banner below)
###############################################################################
"""

# --- LAUNCH MODE CONFIGURATION ---
LAUNCH_MODE = "PYTHON"       # Options: "PYTHON", "DOCKER_COMPOSE", or "RAW"
TARGET = "main.py"           # PYTHON: "main.py"/"bot.py" | DOCKER_COMPOSE: "-f prod.yml" (Optional)
CUSTOM_RAW_COMMAND = ""      # Used ONLY if LAUNCH_MODE is set to "RAW"

# --- Infrastructure & Distributed Lock Properties ---
SERVICE_ID = "telegram-bot-cluster"   # MANDATORY: Explicit unique cluster lock key
DATABASE_NAME = "Failover"
COLLECTION_NAME = "Services"

# --- Cluster Timing Guardrails ---
HEARTBEAT_INTERVAL = 15
HEARTBEAT_TIMEOUT = 60      # Must be >= 3-4x interval to insulate against network transit jitter
CHECK_INTERVAL = 5
LOCAL_RETRY_LIMIT = 3
STARTUP_GRACE_PERIOD = 5    # Delay verification until process finishes internal startup hooks
MAX_NETWORK_GRACE_S = 30    # Continuous database blackout window allowed before stepping down

import os
import sys
import time
import uuid
import shlex
import random
import shutil
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
# 1. PERSISTENT GLOBAL MACHINE IDENTITY & MODE VECTOR INTERPRETATION
# ---------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
IS_WINDOWS = platform.system() == "Windows"

if not SERVICE_ID.strip() or not MONGO_URI or not MONGO_URI.strip():
    print("CRITICAL CONFIGURATION ERROR: Missing core environment variables or SERVICE_ID!", file=sys.stderr)
    sys.exit(1)

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))

# Natively isolate the tracking files away from the project directory to survive directory clones
SYSTEM_HOME = os.path.expanduser("~")
MACHINE_ID_FILE = os.path.join(SYSTEM_HOME, f".ha_watchdog_{SERVICE_ID}.id")

if os.path.exists(MACHINE_ID_FILE):
    try:
        with open(MACHINE_ID_FILE, "r", encoding="utf-8") as f:
            PERSISTENT_MACHINE_UUID = f.read().strip()
    except Exception:
        PERSISTENT_MACHINE_UUID = str(uuid.uuid4())
else:
    PERSISTENT_MACHINE_UUID = str(uuid.uuid4())
    try:
        with open(MACHINE_ID_FILE, "w", encoding="utf-8") as f:
            f.write(PERSISTENT_MACHINE_UUID)
    except Exception as e:
        print(f"WARNING: Unable to write machine identity token locally: {e}", file=sys.stderr)

# Unique Node identifier for transactional integrity
NODE_ID = f"{PERSISTENT_MACHINE_UUID}:{PROJECT_PATH}"

# Short, human-readable alias for simple administrative database overrides
PATH_HASH = hashlib.sha1(PROJECT_PATH.encode('utf-8')).hexdigest()[:8]
HOSTNAME = socket.gethostname()
NODE_ALIAS = f"{HOSTNAME}:{PATH_HASH}"

# --- UNIFIED RESOLVED VECTOR INTERPRETATION ---
final_exec_args = []
mode_upper = LAUNCH_MODE.strip().upper()

if mode_upper == "PYTHON":
    if not TARGET.strip():
        for candidate in ["main.py", "bot.py", "app.py"]:
            if os.path.exists(os.path.join(PROJECT_PATH, candidate)):
                TARGET = candidate
                break
        else:
            TARGET = "main.py"
    final_exec_args = [sys.executable, TARGET]

elif mode_upper == "DOCKER_COMPOSE":
    DOCKER_BIN = shutil.which("docker") or "docker"
    additional_flags = shlex.split(TARGET)
    # Global options must stand after the compose command statement block
    final_exec_args = [DOCKER_BIN, "compose"] + additional_flags + ["up"]

elif mode_upper == "RAW":
    if not CUSTOM_RAW_COMMAND.strip():
        print("CRITICAL LOGIC ERROR: LAUNCH_MODE is set to 'RAW' but 'CUSTOM_RAW_COMMAND' is empty!", file=sys.stderr)
        sys.exit(1)
    final_exec_args = shlex.split(CUSTOM_RAW_COMMAND)

START_COMMAND_STRING = " ".join(f'"{arg}"' for arg in final_exec_args)

if os.getenv("PYCHARM_HOSTED"):
    IDE_CONTEXT = "PyCharm"
elif os.getenv("VSCODE_PID"):
    IDE_CONTEXT = "VS Code"
else:
    IDE_CONTEXT = "Terminal/Shell"

CONFIG_PAYLOAD = f"{START_COMMAND_STRING}|{HEARTBEAT_INTERVAL}|{HEARTBEAT_TIMEOUT}"
CONFIG_FINGERPRINT = hashlib.sha256(CONFIG_PAYLOAD.encode('utf-8')).hexdigest()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node: %(node_alias)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class NodeLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs["extra"] = {"node_alias": NODE_ALIAS}
        return msg, kwargs

logger = NodeLoggerAdapter(logging.getLogger("failover_watchdog"), {})

child_process = None
is_running = True
db_disconnect_tracker = None

try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000, retryReads=True, retryWrites=True)
    db_collection = mongo_client[DATABASE_NAME][COLLECTION_NAME]
except Exception as init_err:
    print(f"CRITICAL: Failed to initialize PyMongo pool: {init_err}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# 2. SYSTEM SIGNAL INTERCEPTION & CONTAINER-AWARE LIFECYCLE
# ---------------------------------------------------------------------------
def handle_shutdown_signal(signum, frame):
    global is_running
    logger.info(f"Received termination signal ({signal.Signals(signum).name}). Cleaning local environment...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)

def terminate_child():
    global child_process
    if LAUNCH_MODE.strip().upper() == "DOCKER_COMPOSE":
        logger.warning("Executing proactive Docker Compose stack teardown sequence...")
        try:
            down_args = list(final_exec_args)
            if down_args[-1] == "up" and "compose" in down_args:
                down_args[-1] = "down"
            else:
                down_args = [shutil.which("docker") or "docker", "compose", "down"]
            subprocess.run(down_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20, check=False)
            logger.info("Docker infrastructure successfully verified down.")
        except Exception as e:
            logger.error(f"Failed to cleanly invoke stack teardown sequence: {e}")

    if child_process and child_process.poll() is None:
        if IS_WINDOWS:
            try:
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(child_process.pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            except Exception as e:
                logger.error(f"Windows taskkill tree termination failed: {e}")
        else:
            try:
                os.killpg(os.getpgid(child_process.pid), signal.SIGTERM)
                for _ in range(5):
                    if child_process.poll() is not None:
                        break
                    time.sleep(1)
                else:
                    os.killpg(os.getpgid(child_process.pid), signal.SIGKILL)
                    child_process.wait()
            except Exception as e:
                logger.error(f"POSIX process tree kill sequence failed: {e}")
    child_process = None

# ---------------------------------------------------------------------------
# 3. TRANSITIONAL LEADER ELECTORATE (DETERMINISTIC LEASE MANAGEMENT)
# ---------------------------------------------------------------------------
def setup_database_indexes():
    try:
        db_collection.create_index([("owner_node_id", pymongo.ASCENDING)], background=True)
        return True
    except PyMongoError as e:
        logger.error(f"Failed to optimize indexing configuration: {e}")
        return False

def bootstrap_and_validate_lock():
    try:
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            initial_state = {
                "_id": SERVICE_ID,
                "owner_node_id": None,
                "forced_leader_node": None,
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
            logger.critical("🚨 CONFIGURATION FINGERPRINT MISMATCH! Execution immediately halted.")
            sys.exit(1)
        return True
    except (ConnectionFailure, PyMongoError) as e:
        logger.error(f"Error checking cluster validation status: {e}. Retrying pool in 5s...")
        time.sleep(5)
        return False

def release_leadership():
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
        logger.info("Released leadership lock successfully.")
    except Exception as e:
        logger.error(f"Failed to issue clean leadership release: {e}")

def try_acquire_or_maintain_leadership(force_check_only=False, update_telemetry=False):
    """
    Acquires or maintains leadership using remote server time filters.
    Respects administrative 'forced_leader_node' aliases atomically.
    """
    global db_disconnect_tracker

    try:
        # Caching optimization: single read footprint used for the full cycle evaluation
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            return False
            
        forced_leader = doc.get("forced_leader_node")

        if force_check_only:
            db_disconnect_tracker = None 
            is_owner = (doc.get("owner_node_id") == NODE_ID)
            
            # If an administrative override targets another machine alias, step down immediately
            if is_owner and forced_leader and forced_leader != NODE_ALIAS:
                logger.warning(f"Administrative override active! Leadership forced to node: '{forced_leader}'. Stepping down.")
                return False
                
            return is_owner

        # STRICT ATOMIC FILTER CONDITIONAL MATCHER:
        # If an override points to this machine's explicit alias, bind the filter strictly to that value
        # to guarantee execution synchronization across rapid database changes.
        if forced_leader:
            if forced_leader != NODE_ALIAS:
                return False
            filter_query = {"_id": SERVICE_ID, "forced_leader_node": NODE_ALIAS}
        else:
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
                "config_fingerprint": CONFIG_FINGERPRINT
            },
            "$currentDate": {
                "last_heartbeat": True
            }
        }

        if update_telemetry:
            update_modifier["$set"].update({
                "project_path": PROJECT_PATH,
                "launch_mode": LAUNCH_MODE,
                "node_alias": NODE_ALIAS,
                "context": IDE_CONTEXT,
                "hostname": HOSTNAME
            })

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
    print(f"🔥 HA PROCESS WATCHDOG ACTIVE | Mode: {LAUNCH_MODE}", flush=True)
    print(f"Service ID : {SERVICE_ID}", flush=True)
    print(f"NODE ALIAS : {NODE_ALIAS}", flush=True) # Explicit key used for manual overrides
    print(f"Vector     : {final_exec_args}", flush=True)
    print(f"======================================================================\n", flush=True)
    
    time.sleep(random.uniform(0.5, 3.5))

    if not setup_database_indexes() or not bootstrap_and_validate_lock():
        return

    is_leader = False
    local_failures = 0
    last_heartbeat_time = 0

    while is_running:
        try:
            # --- STANDBY MONITORING LAYER ---
            if not is_leader:
                if not try_acquire_or_maintain_leadership(update_telemetry=True):
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
                            terminate_child() 
                            release_leadership()
                            is_leader = False
                            time.sleep(CHECK_INTERVAL)
                            continue

                    if not try_acquire_or_maintain_leadership():
                        logger.warning("Split-brain caught during crash recovery phase. Reverting to standby.")
                        is_leader = False
                        continue

                    logger.info(f"Executing application array vector: {final_exec_args}")
                    try:
                        if IS_WINDOWS:
                            child_process = subprocess.Popen(final_exec_args, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
                        else:
                            child_process = subprocess.Popen(final_exec_args, start_new_session=True)
                            
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
                
                # Check for split-brain or manual override every second
                if not try_acquire_or_maintain_leadership(force_check_only=True):
                    logger.critical("🚨 STEPDOWN TRIGGERED: Node identity overtaken or manual override active! Stopping local application.")
                    terminate_child()
                    is_leader = False
                    continue

                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    if child_process.poll() is None:
                        if try_acquire_or_maintain_leadership(update_telemetry=False):
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
