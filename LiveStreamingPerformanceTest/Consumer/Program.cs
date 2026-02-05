using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using Microsoft.AspNet.SignalR.Client;
using Newtonsoft.Json;
using System.Text;

namespace SignalRPublisher
{
    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string SIGNALR_URL = "http://localhost:8081/signalr";
        private static string logFile = $"signalr-publisher-{DateTime.Now:yyyyMMdd-HHmmss}.log";

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
            var connection = new HubConnection(SIGNALR_URL);
            var hubProxy = connection.CreateHubProxy("PerformanceTestHub");

            LogMessage($"Connecting to SignalR hub at {SIGNALR_URL}");

            try
            {
                await connection.Start();
                LogMessage("Connected to SignalR hub");
                await Task.Delay(1000);

                // Warm-up phase
                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages");
                await SendMessages(hubProxy, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up phase completed");

                await Task.Delay(2000);

                // Actual test phase
                LogMessage($"Starting performance test with {TEST_MESSAGES} messages");
                var stopwatch = Stopwatch.StartNew();
                await SendMessages(hubProxy, TEST_MESSAGES, "TEST");
                stopwatch.Stop();

                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;
                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms");
                LogMessage($"Throughput: {throughput:F2} messages/second");

                await Task.Delay(5000);
            }
            finally
            {
                connection.Stop();
                connection.Dispose();
            }
        }

        private static async Task SendMessages(IHubProxy hubProxy, int messageCount, string phase)
        {
            // Measure base JSON size without content
            var emptyMessage = new
            {
                Id = 1,
                Phase = phase,
                Timestamp = DateTime.UtcNow.Ticks,
                Content = string.Empty
            };
            var baseJson = JsonConvert.SerializeObject(emptyMessage);
            var baseJsonSize = Encoding.UTF8.GetByteCount(baseJson);

            // Calculate padding needed to reach 6KB (6144 bytes)
            const int targetSize = 6144;
            int paddingNeeded = targetSize - baseJsonSize;
            if (paddingNeeded < 0) throw new InvalidOperationException("Base message too large for 6KB target!");

            for (int i = 1; i <= messageCount; i++)
            {
                // Pad content to reach target size
                var paddedContent = $"Message {i}".PadRight(paddingNeeded, 'X');
                var message = new
                {
                    Id = i,
                    Phase = phase,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Content = paddedContent
                };

                try
                {
                    await hubProxy.Invoke("SendMessage", message);
                    if (i % 100 == 0)
                    {
                        await Task.Delay(10);
                        LogMessage($"Sent {i} {phase} messages...");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to send message {i}: {ex.Message}");
                }
            }

            LogMessage($"Completed sending all {messageCount} {phase} messages");
        }

        private static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";
            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }
}