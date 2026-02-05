# Real-Time Streaming Performance Comparison

## Overview
This project compares the performance of three real-time streaming technologies: **WebSockets**, **Apache Kafka**, and **Redpanda**. The goal is to understand their behavior under different workloads, evaluate latency, throughput, scalability, and suitability for different scenarios.

The solution contains **6 projects**: three producer-consumer pairs implementing each streaming technology. Kafka and Redpanda run as Docker containers, while WebSockets runs natively in .NET.

The project was developed using **.NET 8.0**.

## Architecture & Setup
- **Producers and Consumers:** 3 pairs (WebSockets, Kafka, Redpanda)
- **Docker Setup:** Apache Kafka + Redpanda
- **Test Workloads:** 1, 100, 1,000, 10,000, 1,000,000 messages
- **Documentation:** Screenshots with timestamps are included in `Live Streamers.docx`

## Test Methodology
1. Choose the number of messages to send by modifying the variable in the Producer code
2. Run the **consumer** first, then the **producer**.
3. Results are automatically logged into a local Excel sheet with timestamps.
4. Compare performance across technologies using tables for latency, throughput, and other key metrics.


## Comparative Analysis
The detailed comparisons are available in the Word document, including:

- ### Pros vs Concerns
- ### Core Comparison
- ### License & Support
- ### Extra Features


## Key Observations
- WebSockets excels in low-latency, real-time UI scenarios but struggles with scale and durability.  
- Kafka provides durability and decoupling at the cost of operational complexity.  
- Redpanda shows the lowest latency and highest throughput while remaining Kafka-compatible.

