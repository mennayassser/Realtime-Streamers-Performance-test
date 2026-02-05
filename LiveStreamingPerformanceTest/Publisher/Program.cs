using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using Microsoft.AspNet.SignalR.Client;
using System.Threading;

namespace SignalRPublisher
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long Timestamp { get; set; }
        public string Content { get; set; }
    }

    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const int BATCH_SIZE = 100; // Send messages in batches
        private const int PARALLELISM = 8;  // Number of parallel senders
        private const string SIGNALR_URL = "http://localhost:8081/signalr";
        private static string logFile = $"signalr-publisher-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly SemaphoreSlim LogSemaphore = new SemaphoreSlim(1, 1);

        static async Task Main(string[] args)
        {
            LogMessage("SignalR Publisher Starting...");

            Console.WriteLine("Make sure SignalR Subscriber is running and ready.");
            Console.WriteLine("Press any key to start the performance test...");
            Console.ReadKey();

            try
            {
                await RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task RunPerformanceTest()
        {
            var connection = new HubConnection(SIGNALR_URL)
            {
                // Increase transport connect timeout for reliability under load
                TransportConnectTimeout = TimeSpan.FromSeconds(30)
            };
            var hubProxy = connection.CreateHubProxy("PerformanceTestHub");

            LogMessage($"Connecting to SignalR hub at {SIGNALR_URL}");

            try
            {
                await connection.Start();
                LogMessage("Connected to SignalR hub");

                await Task.Delay(1000);

                LogMessage($"Creating {WARM_UP_MESSAGES} warm-up messages");
                var warmupMessages = CreateMessages(WARM_UP_MESSAGES, "WARMUP");

                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages");
                await SendMessagesParallel(hubProxy, warmupMessages);
                LogMessage("Warm-up phase completed");

                await Task.Delay(2000);

                LogMessage($"Creating {TEST_MESSAGES} test messages");
                var testMessages = CreateMessages(TEST_MESSAGES, "TEST");
                LogMessage($"All {TEST_MESSAGES} test messages created, starting to send...");

                LogMessage($"Starting performance test with {TEST_MESSAGES} messages");
                var stopwatch = Stopwatch.StartNew();

                await SendMessagesParallel(hubProxy, testMessages);

                stopwatch.Stop();
                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;

                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms");
                LogMessage($"Throughput: {throughput:F2} messages/second");

                LogMessage("Waiting for subscriber to process all messages...");
                await Task.Delay(5000);
            }
            finally
            {
                connection.Stop();
                connection.Dispose();
            }
        }

        private static List<Message> CreateMessages(int messageCount, string phase)
        {
            var messages = new List<Message>(messageCount);

            for (int i = 1; i <= messageCount; i++)
            {
                var message = new Message
                {
                    Id = i,
                    Phase = phase,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Content = $"Message {i} from SignalR Publisher"
                };

                messages.Add(message);
            }

            return messages;
        }

        // Parallel and batched sending for maximum throughput
        private static async Task SendMessagesParallel(IHubProxy hubProxy, List<Message> messages)
        {
            int total = messages.Count;
            int sent = 0;
            var tasks = new List<Task>();

            for (int batchStart = 0; batchStart < total; batchStart += BATCH_SIZE)
            {
                var batch = messages.GetRange(batchStart, Math.Min(BATCH_SIZE, total - batchStart));
                tasks.Add(Task.Run(async () =>
                {
                    foreach (var message in batch)
                    {
                        try
                        {
                            await hubProxy.Invoke("SendMessage", message);
                        }
                        catch (Exception ex)
                        {
                            LogMessage($"Failed to send message {message.Id}: {ex.Message}");
                        }
                    }
                }));

                if (tasks.Count >= PARALLELISM)
                {
                    await Task.WhenAll(tasks);
                    sent += tasks.Count * BATCH_SIZE;
                    LogMessage($"Sent {Math.Min(sent, total)} {batch[0].Phase} messages...");
                    tasks.Clear();
                }
            }

            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
                sent += tasks.Count * BATCH_SIZE;
                LogMessage($"Sent {Math.Min(sent, total)} {messages[0].Phase} messages...");
            }

            LogMessage($"Completed sending all {messages.Count} {messages[0].Phase} messages");
        }

        // Async logging to avoid blocking sender threads
        private static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";

            Console.WriteLine(logEntry);
            Task.Run(async () =>
            {
                await LogSemaphore.WaitAsync();
                try
                {
                    File.AppendAllText(logFile, logEntry + Environment.NewLine);
                }
                finally
                {
                    LogSemaphore.Release();
                }
            });
        }
    }
}