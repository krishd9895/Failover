#!/usr/bin/env python3
"""
###############################################################################
# PRODUCTION-READY HIGH-AVAILABILITY PROCESS WATCHDOG
#
# Core Mechanics:
# 1. Reliable, cross-platform process isolation (POSIX & Windows).
# 2. Strict remote server-side clock evaluation using $currentDate.
# 3. Directory-derived node identity tracking using the script path.
###############################################################################
"""

SERVICE_ID = "telegram ABC bot"
START_COMMAND = "python main.py"

# --- Infrastructure & Database Naming Layout ---
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
# 1. ENVIRONMENT & DETERMINISTIC NODE IDENTITY INITIALIZATION
# ---------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
IS_WINDOWS = platform.system() == "Windows"

# Always points to the explicit folder containing failover.py, regardless of execution working directory
PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
HOSTNAME = socket.gethostname()

# Explicit, fully human-readable identity string for straightforward debugging
NODE_ID = f"{HOSTNAME}:{PROJECT_PATH}"

if not SERVICE_ID.strip() or not START_COMMAND.strip() or not MONGO_URI or not MONGO_URI.strip():
    print("CRITICAL CONFIGURATION ERROR: Missing core environment variables!", file=sys.stderr)
    sys.exit(1)

# Detect runtime host environment purely for informational context
if os.getenv("PYCHARM_HOSTED"):
    IDE_CONTEXT = "PyCharm"
elif os.getenv("VSCODE_PID"):
    IDE_CONTEXT = "VS Code"
else:
    IDE_CONTEXT = "Terminal/Shell"

# Configuration fingerprint to prevent cross-project configuration collisions
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

# Establish single, long-lived MongoClient connection pool
try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000, retryReads=True, retryWrites=True)
    db_collection = mongo_client[DATABASE_NAME][COLLECTION_NAME]
except Exception as init_err:
    print(f"CRITICAL: Failed to initialize PyMongo pool: {init_err}", file=sys.stderr)
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
                # Forceful, recursive child-tree termination via native Windows CLI
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
    """Runs exactly once during startup initialization path."""
    try:
        db_collection.create_index([("owner_node_id", pymongo.ASCENDING)], background=True)
        return True
    except PyMongoError as e:
        logger.error(f"Failed to optimize indexing configuration: {e}")
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
            logger.critical("🚨 CONFIGURATION FINGERPRINT MISMATCH! Execution immediately halted.")
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

        # Highly compatible standard expression evaluation matching server-side time threshold
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
    
    print(f"HA WATCHDOG ACTIVE | Service: {SERVICE_ID} | Node: {NODE_ID} ({IDE_CONTEXT})\n", flush=True)
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
                
                # Double-check configuration stability right upon capturing leadership state
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
                        # Achieves clean, parallel process-tree isolation on both Windows and POSIX variants
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
                
                # Check for split-brain scenario every second
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
                        logger.warning("Supervised process died inside the scheduled pulse window.")

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

