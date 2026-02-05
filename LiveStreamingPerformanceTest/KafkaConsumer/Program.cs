using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace KafkaConsumer
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long StartTimestamp { get; set; }
        public string Content { get; set; }
    }

    public class LatencyMeasurement
    {
        public int MessageId { get; set; }
        public double LatencyMs { get; set; }
        public long StartTimestamp { get; set; }
        public long EndTimestamp { get; set; }
    }

    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new List<LatencyMeasurement>();
        private const string KAFKA_TOPIC = "performance-test";
        private const string KAFKA_BROKER_HOST = "localhost";
        private const int KAFKA_BROKER_PORT = 9092;
        private const int expectedTestMessages = 10000;
        private static int receivedTestMessages = 0;
        private static bool testCompleted = false;
        private static bool calculationsComplete = false;
        private static string logFile = $"kafka-consumer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        static void Main(string[] args)
        {
            LogMessage("Kafka Consumer Starting...");

            try
            {
                StartKafkaConsumer().Wait();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task StartKafkaConsumer()
        {
            var options = new KafkaOptions(new Uri($"http://{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"))
            {
                Log = new ConsoleLog(),
                ResponseTimeoutMs = TimeSpan.FromSeconds(30),
            };

            var router = new BrokerRouter(options);
            var consumer = new Consumer(new ConsumerOptions(KAFKA_TOPIC, router)
            {
                Log = new ConsoleLog()
            });

            try
            {
                LogMessage($"Kafka consumer subscribed to topic: {KAFKA_TOPIC}");
                LogMessage("Waiting for messages...");

                await Task.Run(() =>
                {
                    foreach (var message in consumer.Consume(cancellationTokenSource.Token))
                    {
                        if (calculationsComplete)
                            break;

                        ProcessMessage(message);
                    }
                });
            }
            catch (OperationCanceledException)
            {
                LogMessage("Consumer operation was cancelled");
            }
            finally
            {
                consumer.Dispose();
                router.Dispose();
                LogMessage("Kafka consumer closed");
            }
        }

        private static void ProcessMessage(KafkaNet.Protocol.Message kafkaMessage)
        {
            try
            {
                var endTimestamp = DateTime.UtcNow.Ticks;

                var messageJson = Encoding.UTF8.GetString(kafkaMessage.Value);
                var message = JsonConvert.DeserializeObject<Message>(messageJson);
                if (message == null) return;

                var latencyTicks = endTimestamp - message.StartTimestamp;
                var latencyMs = new TimeSpan(latencyTicks).TotalMilliseconds;

                if (message.Phase == "TEST")
                {
                    lock (Latencies)
                    {
                        Latencies.Add(new LatencyMeasurement
                        {
                            MessageId = message.Id,
                            LatencyMs = latencyMs,
                            StartTimestamp = message.StartTimestamp,
                            EndTimestamp = endTimestamp
                        });

                        receivedTestMessages++;

                        if (receivedTestMessages % 1000 == 0)
                        {
                            LogMessage($"Received {receivedTestMessages} test messages so far...");
                        }

                        if (receivedTestMessages >= expectedTestMessages && !testCompleted)
                        {
                            testCompleted = true;
                            LogMessage("All test messages received. Calculating statistics...");
                            Task.Run(() => CalculateAndLogStatistics());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Error processing message: {ex.Message}");
            }
        }

        private static void CalculateAndLogStatistics()
        {
            if (Latencies.Count == 0)
            {
                LogMessage("WARNING: No latency measurements collected");
                calculationsComplete = true;
                cancellationTokenSource.Cancel();
                return;
            }

            var sortedLatencies = Latencies.Select(l => l.LatencyMs).OrderBy(l => l).ToArray();

            var min = sortedLatencies.First();
            var max = sortedLatencies.Last();
            var mean = sortedLatencies.Average();
            var median = GetPercentile(sortedLatencies, 50);
            var p95 = GetPercentile(sortedLatencies, 95);
            var p99 = GetPercentile(sortedLatencies, 99);

            var firstMessage = Latencies.OrderBy(l => l.StartTimestamp).First();
            var lastMessage = Latencies.OrderBy(l => l.EndTimestamp).Last();
            var testDurationSeconds = new TimeSpan(lastMessage.EndTimestamp - firstMessage.StartTimestamp).TotalSeconds;
            var throughput = testDurationSeconds > 0 ? Latencies.Count / testDurationSeconds : 0;

            LogMessage("=== Kafka Performance Results ===");
            LogMessage($"Total Messages: {Latencies.Count}");
            LogMessage($"Test Duration: {testDurationSeconds:F2} seconds");
            LogMessage($"Throughput: {throughput:F2} messages/second");
            LogMessage("Latency Statistics (ms):");
            LogMessage($"  Min: {min:F3}");
            LogMessage($"  Max: {max:F3}");
            LogMessage($"  Mean: {mean:F3}");
            LogMessage($"  Median: {median:F3}");
            LogMessage($"  95th Percentile: {p95:F3}");
            LogMessage($"  99th Percentile: {p99:F3}");

            SaveResultsToCsv();

            calculationsComplete = true;
            cancellationTokenSource.Cancel();
        }

        private static double GetPercentile(double[] sortedArray, int percentile)
        {
            if (sortedArray.Length == 0) return 0;

            var index = (percentile / 100.0) * (sortedArray.Length - 1);
            var lower = (int)Math.Floor(index);
            var upper = (int)Math.Ceiling(index);

            if (lower == upper)
                return sortedArray[lower];

            var weight = index - lower;
            return sortedArray[lower] * (1 - weight) + sortedArray[upper] * weight;
        }

        private static void SaveResultsToCsv()
        {
            var csvPath = $"kafka-results-{DateTime.Now:yyyyMMdd-HHmmss}.csv";

            using (var writer = new StreamWriter(csvPath))
            {
                writer.WriteLine("MessageId,LatencyMs,StartTimestamp,EndTimestamp");

                foreach (var measurement in Latencies.OrderBy(l => l.MessageId))
                {
                    writer.WriteLine($"{measurement.MessageId},{measurement.LatencyMs:F3},{measurement.StartTimestamp},{measurement.EndTimestamp}");
                }
            }

            LogMessage($"Detailed results saved to: {csvPath}");
        }

        private static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";

            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }

    public class ConsoleLog : IKafkaLog
    {
        public void DebugFormat(string format, params object[] args)
        {
        }

        public void InfoFormat(string format, params object[] args)
        {
        }

        public void WarnFormat(string format, params object[] args)
        {
            Console.WriteLine("WARN: " + string.Format(format, args));
        }

        public void ErrorFormat(string format, params object[] args)
        {
            Console.WriteLine("ERROR: " + string.Format(format, args));
        }

        public void FatalFormat(string format, params object[] args)
        {
            Console.WriteLine("FATAL: " + string.Format(format, args));
        }
    }
}