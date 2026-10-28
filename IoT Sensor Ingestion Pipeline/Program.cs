using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// 1. Data Model representing an IoT sensor reading
public record SensorReading(string DeviceId, double Temperature, DateTime Timestamp);

// 2. Manual Bounded Producer-Consumer Queue Engine
public class SensorDataPipeline
{
    private readonly Queue<SensorReading> _queue = new();
    private readonly int _maxSize;
    private readonly SemaphoreSlim _itemsAvailable;
    private readonly SemaphoreSlim _spaceAvailable;
    private readonly object _lock = new();

    public SensorDataPipeline(int maxSize)
    {
        _maxSize = maxSize;
        _itemsAvailable = new SemaphoreSlim(0);
        _spaceAvailable = new SemaphoreSlim(maxSize);
    }

    // Producer method: Adds sensor data to the queue with backpressure
    public async Task ProduceAsync(string deviceId, double temp, CancellationToken ct)
    {
        // Wait if the queue is full (Backpressure)
        await _spaceAvailable.WaitAsync(ct);

        lock (_lock)
        {
            var reading = new SensorReading(deviceId, temp, DateTime.Now);
            _queue.Enqueue(reading);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[PRODUCER] {deviceId} pushed {temp:F1}°C. Queue size: {_queue.Count}/{_maxSize}");
            Console.ResetColor();
        }

        // Signal that a new item is available for consumers
        _itemsAvailable.Release();
    }

    // Consumer method: Removes and processes sensor data from the queue
    public async Task ConsumeAsync(int workerId, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Wait until at least one item is in the queue
                await _itemsAvailable.WaitAsync(ct);

                SensorReading reading;
                lock (_lock)
                {
                    reading = _queue.Dequeue();

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"  [CONSUMER #{workerId}] Processed telemetry from {reading.DeviceId} -> {reading.Temperature:F1}°C");
                    Console.ResetColor();
                }

                //Signal that space has opened up in the queue
                _spaceAvailable.Release();

                // Simulate processing time (e.g., writing to database or running analytics)
                await Task.Delay(400, ct); 
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}

//3. System Execution Simulation
class Program
{
    static async Task Main(string[] args)
    {
        // Create a bounded pipeline with a maximum capacity of 4 items
        var pipeline = new SensorDataPipeline(maxSize: 4);
        var cts = new CancellationTokenSource();

        // Spin up 2 background consumers (workers) processing data concurrently
        var consumerTasks = new[]
        {
            Task.Run(() => pipeline.ConsumeAsync(1, cts.Token)),
            Task.Run(() => pipeline.ConsumeAsync(2, cts.Token))
        };

        Console.WriteLine("--- IoT SENSOR PIPELINE STARTED ---\n");

        // Simulate 3 different IoT devices acting as fast producers
        var producerTask1 = SimulateDeviceAsync(pipeline, "Sensor-Alpha-01", 10, cts.Token);
        var producerTask2 = SimulateDeviceAsync(pipeline, "Sensor-Beta-02", 8, cts.Token);

        // Wait for producers to finish publishing their data batches
        await Task.WhenAll(producerTask1, producerTask2);

        // Allow consumers a moment to clear remaining items in the queue
        await Task.Delay(2000);

        // Graceful shutdown
        cts.Cancel();
        await Task.WhenAll(consumerTasks);

        Console.WriteLine("\n--- PIPELINE SHUTDOWN CLEANLY ---");
    }

    static async Task SimulateDeviceAsync(SensorDataPipeline pipeline, string deviceId, int count, CancellationToken ct)
    {
        var rnd = new Random();
        for (int i = 0; i < count; i++)
        {
            double temp = 20.0 + rnd.NextDouble() * 15.0; // Random temp between 20-35°C
            await pipeline.ProduceAsync(deviceId, temp, ct);

            // Producer run faster than consumers to demonstrate backpressure/blocking
            await Task.Delay(150, ct);
        }
    }
}
