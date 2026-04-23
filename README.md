# Big Data Flight Analysis: Hadoop MapReduce Pipeline

## Overview
This project processes and analyzes 22 years of airline on-time performance data (1987–2008) using a distributed Hadoop cluster. Designed to extract actionable performance metrics and ensure data integrity within the aviation sector, the pipeline evaluates flight delays, taxi times, route distances, and cancellation trends across millions of records. 

To overcome Oozie dependency constraints, the workflow execution was optimized using a custom Bash scheduling script that sequentially triggers MapReduce jobs and tracks execution times.

## Infrastructure & Tech Stack
* **Compute Framework:** Apache Hadoop 3.3.6, MapReduce, Java 8 
* **Cloud Infrastructure:** AWS EC2 (t3.large instances, 30GB gp3 storage)
* **Cluster Scale:** Evaluated performance scaling from a 2-node baseline up to a 7-node cluster (1 Master + 6 Workers).
* **Automation:** Custom Bash shell scripting for sequential job execution and HDFS output management.

## MapReduce Architecture

The analysis is broken down into four distinct, fully distributed MapReduce jobs. A custom `IntPairWritable` class was implemented to pass multiple integers (e.g., metric sums and flight counts) from the Mapper to the Reducer, enabling single-pass calculations for averages and probabilities.

1. **On-Time Airline Analysis:** Calculates the probability of an airline arriving on schedule (delay $\le$ 10 minutes).
2. **Airport Taxi Time:** Computes the average total taxi time (Taxi-In + Taxi-Out) for all airports.
3. **Cancellation Reasons:** Aggregates occurrences of specific flight cancellation codes (Carrier, Weather, NAS, Security).
4. **Average Route Distance:** Identifies the top 3 specific flight routes with the longest average travel distance using an in-memory HashMap sorting phase during Reducer cleanup.

## Key Findings & Cluster Performance

### Analytical Insights
* **Top On-Time Airlines:** Hawaiian Airlines (HA) led with a ~92.16% on-time probability, followed by Aloha Airlines (AQ).
* **Longest Routes:** The routes between Honolulu (HNL) and JFK consistently averaged the longest distance at 4,983 miles.
* **Cancellations:** Carrier-related issues (Code A) were the leading cause of cancellations (289,613 incidents).

### Hardware Scaling Observations
The cluster scaling analysis demonstrated classic Hadoop scalability alongside the law of diminishing returns:
* **Major Bottleneck Resolution:** Scaling from 2 VMs to 3 VMs yielded the most dramatic speed boost (e.g., Taxi Time processing dropped from 16m 58s to 11m 33s).
* **Diminishing Returns:** Expanding from 6 VMs to 7 VMs provided marginal improvements (e.g., Cancellation job saved only 2 seconds), indicating that network shuffle overhead began outweighing raw CPU additions. A 4-5 node cluster was identified as the optimal resource-to-speed ratio.
