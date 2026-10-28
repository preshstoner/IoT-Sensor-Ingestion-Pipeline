# IoT Sensor Data Ingestion Pipeline 📡⚡

A multi-threaded, concurrent C# console application implementing the **Producer-Consumer Pattern** from scratch using native low-level synchronization primitives (`Queue<T>`, `SemaphoreSlim`, and `lock`). 

Designed to demonstrate how bounded queues enforce **backpressure** and prevent memory overload when fast producers outpace slow consumers.

---

## 🌟 Key Features

* **Manual Concurrency Control:** Combines `SemaphoreSlim` for async counting/signaling with standard `lock` statements for thread-safe queue access.
* **Bounded Capacity & Backpressure:** Enforces a strict maximum queue size (`MaxSize`). When the queue fills up, producers automatically block and wait until consumers free up space.
* **Multi-Producer / Multi-Consumer Support:** Multiple simulated IoT hardware devices push data concurrently while multiple background workers pull and process telemetry data.
* **Graceful Shutdown:** Utilizes `CancellationToken` workflows to shut down worker loops cleanly without leaving dangling threads.

---

## 📂 Architecture Overview

* **`SensorReading`**: Immutable record storing telemetry metadata (Device ID, temperature reading, timestamp).
* **`SensorDataPipeline`**: Core engine containing the shared `Queue<T>`, exclusive `lock`, and dual `SemaphoreSlim` counters (`_itemsAvailable` and `_spaceAvailable`).
* **`Program`**: Orchestrates concurrent sensor producers and background data consumers.

---

## 🚀 Getting Started

### Prerequisites
* [.NET 8.0 SDK or higher](https://dotnet.microsoft.com/)

### Running the Application
1. Clone the repository and navigate into the project folder.
2. Run the application using the .NET CLI:
   ```powershell
   dotnet run
