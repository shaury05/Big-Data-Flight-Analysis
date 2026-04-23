# Big Data Flight Analysis: Hadoop MapReduce Pipeline

## Overview
[cite_start]This project processes and analyzes 22 years of airline on-time performance data (1987–2008) using a distributed Hadoop cluster[cite: 2874, 3660]. Designed to extract actionable performance metrics and ensure data integrity within the aviation sector, the pipeline evaluates flight delays, taxi times, route distances, and cancellation trends across millions of records. 

[cite_start]To overcome Oozie dependency constraints, the workflow execution was optimized using a custom Bash scheduling script that sequentially triggers MapReduce jobs and tracks execution times[cite: 3008, 3662].

## Infrastructure & Tech Stack
* [cite_start]**Compute Framework:** Apache Hadoop 3.3.6, MapReduce, Java 8 [cite: 3659]
* [cite_start]**Cloud Infrastructure:** AWS EC2 (t3.large instances, 30GB gp3 storage) [cite: 3659]
* [cite_start]**Cluster Scale:** Evaluated performance scaling from a 2-node baseline up to a 7-node cluster (1 Master + 6 Workers)[cite: 3549, 3550].
* [cite_start]**Automation:** Custom Bash shell scripting for sequential job execution and HDFS output management[cite: 3662].

## MapReduce Architecture

[cite_start]The analysis is broken down into four distinct, fully distributed MapReduce jobs[cite: 2966]. [cite_start]A custom `IntPairWritable` class was implemented to pass multiple integers (e.g., metric sums and flight counts) from the Mapper to the Reducer, enabling single-pass calculations for averages and probabilities[cite: 3001, 3006].

1. [cite_start]**On-Time Airline Analysis:** Calculates the probability of an airline arriving on schedule (delay $\le$ 10 minutes)[cite: 2967, 2968].
2. [cite_start]**Airport Taxi Time:** Computes the average total taxi time (Taxi-In + Taxi-Out) for all airports[cite: 2978, 2984].
3. [cite_start]**Cancellation Reasons:** Aggregates occurrences of specific flight cancellation codes (Carrier, Weather, NAS, Security)[cite: 2985, 2986].
4. [cite_start]**Average Route Distance:** Identifies the top 3 specific flight routes with the longest average travel distance using an in-memory HashMap sorting phase during Reducer cleanup[cite: 2992, 2998, 2999].

## Key Findings & Cluster Performance

### Analytical Insights
* [cite_start]**Top On-Time Airlines:** Hawaiian Airlines (HA) led with a ~92.16% on-time probability, followed by Aloha Airlines (AQ)[cite: 3630, 3631].
* [cite_start]**Longest Routes:** The routes between Honolulu (HNL) and JFK consistently averaged the longest distance at 4,983 miles[cite: 3639, 3640, 3641].
* [cite_start]**Cancellations:** Carrier-related issues (Code A) were the leading cause of cancellations (289,613 incidents)[cite: 3655, 3656].

### Hardware Scaling Observations
[cite_start]The cluster scaling analysis demonstrated classic Hadoop scalability alongside the law of diminishing returns[cite: 3605, 3614]:
* [cite_start]**Major Bottleneck Resolution:** Scaling from 2 VMs to 3 VMs yielded the most dramatic speed boost (e.g., Taxi Time processing dropped from 16m 58s to 11m 33s)[cite: 3550, 3608, 3610].
* [cite_start]**Diminishing Returns:** Expanding from 6 VMs to 7 VMs provided marginal improvements (e.g., Cancellation job saved only 2 seconds), indicating that network shuffle overhead began outweighing raw CPU additions[cite: 3550, 3615, 3617]. [cite_start]A 4-5 node cluster was identified as the optimal resource-to-speed ratio[cite: 3624].
