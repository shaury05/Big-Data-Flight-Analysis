#!/bin/bash

# --- CONFIGURATION: SET OUTPUT FOLDER FOR TIME ANALYSIS ---
OUTPUT_DIR_SUFFIX="_6nodes"

# Base input and output paths
HDFS_INPUT="/user/ubuntu/input"
HDFS_OUTPUT="/user/ubuntu/output"

echo "========================================================================="
echo " FLIGHT DATA ANALYSIS: SHELL SCRIPT WORKFLOW (6-NODE CLUSTER EXECUTION) "
echo "========================================================================="
echo ""
echo "Starting 4 sequential MapReduce Jobs. Execution time will be measured."

# 1. CLEAN OUTPUT: Clear all previous output directories for this run
echo ">>> STEP 1: Cleaning previous output directories..."
hdfs dfs -rm -r ${HDFS_OUTPUT}/ontime${OUTPUT_DIR_SUFFIX}
hdfs dfs -rm -r ${HDFS_OUTPUT}/taxi${OUTPUT_DIR_SUFFIX}
hdfs dfs -rm -r ${HDFS_OUTPUT}/cancel${OUTPUT_DIR_SUFFIX}
hdfs dfs -rm -r ${HDFS_OUTPUT}/distance${OUTPUT_DIR_SUFFIX}
echo "Cleanup complete."

# 2. JOB EXECUTION & TIMING: Run all four independent jobs sequentially

# Job 1: On-Time Probability (Probability of being on schedule)
echo ""
echo ">>> STEP 2A: Running Job 1 (OnTimeProbability)..."
time hadoop jar ~/airline_jobs/classes/airline.jar OnTimeProbability ${HDFS_INPUT} ${HDFS_OUTPUT}/ontime${OUTPUT_DIR_SUFFIX}

# Job 2: Taxi Time (Average Taxi Time per Airport)
echo ""
echo ">>> STEP 2B: Running Job 2 (TaxiTimePerAirport)..."
time hadoop jar ~/airline_jobs/classes/airline.jar TaxiTimePerAirport ${HDFS_INPUT} ${HDFS_OUTPUT}/taxi${OUTPUT_DIR_SUFFIX}

# Job 3: Cancellation Reason (Count of A, B, C, D codes)
echo ""
echo ">>> STEP 2C: Running Job 3 (CancellationReason)..."
time hadoop jar ~/airline_jobs/classes/airline.jar CancellationReason ${HDFS_INPUT} ${HDFS_OUTPUT}/cancel${OUTPUT_DIR_SUFFIX}

# Job 4: Average Distance (Top 3 Longest Routes)
echo ""
echo ">>> STEP 2D: Running Job 4 (AverageDistance)..."
time hadoop jar ~/airline_jobs/classes/airline.jar AverageDistance ${HDFS_INPUT} ${HDFS_OUTPUT}/distance${OUTPUT_DIR_SUFFIX}

# 3. VERIFICATION: Display summary of execution times and results
echo ""
echo ">>> STEP 3: Workflow complete. View execution times above (real)."
echo "--- Final Results Check (On-Time Probability Top 3) ---"
hdfs dfs -cat ${HDFS_OUTPUT}/ontime${OUTPUT_DIR_SUFFIX}/part-r-00000 | sort -k2 -nr | head -n 3