using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace RedPandaSubscriber
{
    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new List<LatencyMeasurement>();
        private const int EXPECTED_MESSAGES = 10000;
        private static int _receivedCount = 0;
        private static bool _testCompleted = false;
        private static string _logFile = $"redpanda-subscriber-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private const string KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"; // Updated port
        private const string TOPIC_NAME = "performance-test";

        static void Main(string[] args)
        {
            LogMessage("RedPanda Subscriber Starting...");
            try
            {
                StartKafkaConsumer();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}\n{ex.StackTrace}");
            }
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static void StartKafkaConsumer()
        {
            var options = new KafkaOptions(new Uri($"http://{KAFKA_BOOTSTRAP_SERVERS}"));
            var router = new BrokerRouter(options);
            var consumer = new Consumer(new ConsumerOptions(TOPIC_NAME, router));

            try
            {
                LogMessage($"Subscribed to {TOPIC_NAME}");

                // Start message processing loop
                foreach (var message in consumer.Consume())
                {
                    if (_testCompleted) break;

                    var json = Encoding.UTF8.GetString(message.Value);
                    ProcessMessage(json);

                    if (_testCompleted) break;
                }
            }
            finally
            {
                consumer.Dispose();
            }
        }

        private static void ProcessMessage(string json)
        {
            try
            {
                dynamic msg = JsonConvert.DeserializeObject(json);
                if (msg.Phase != "TEST") return;

                long receiveTimestamp = DateTime.UtcNow.Ticks;
                long sentTimestamp = (long)msg.Timestamp;
                double latencyMs = new TimeSpan(receiveTimestamp - sentTimestamp).TotalMilliseconds;

                lock (Latencies)
                {
                    Latencies.Add(new LatencyMeasurement
                    {
                        MessageId = (int)msg.Id,
                        LatencyMs = latencyMs,
                        SentTimestamp = sentTimestamp,
                        ReceivedTimestamp = receiveTimestamp
                    });

                    if (++_receivedCount % 500 == 0)
                        LogMessage($"Received {_receivedCount} test messages");

                    if (_receivedCount >= EXPECTED_MESSAGES)
                    {
                        _testCompleted = true;
                        LogMessage("All messages received. Calculating stats...");
                        CalculateAndLogStatistics();
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Parse error: {ex.Message}");
            }
        }

        private static void CalculateAndLogStatistics()
        {
            if (Latencies.Count == 0)
            {
                LogMessage("No measurements collected");
                return;
            }

            var latencies = Latencies.Select(l => l.LatencyMs).OrderBy(l => l).ToArray();
            long testDurationTicks = Latencies.Max(l => l.ReceivedTimestamp) - Latencies.Min(l => l.SentTimestamp);
            double testDurationSeconds = new TimeSpan(testDurationTicks).TotalSeconds;

            LogMessage("===== PERFORMANCE RESULTS =====");
            LogMessage($"Messages: {Latencies.Count}");
            LogMessage($"Duration: {testDurationSeconds:F2} seconds");
            LogMessage($"Throughput: {Latencies.Count / testDurationSeconds:F2} msg/sec");
            LogMessage("Latency (ms):");
            LogMessage($"  Min: {latencies.First():F2}");
            LogMessage($"  Max: {latencies.Last():F2}");
            LogMessage($"  Mean: {latencies.Average():F2}");
            LogMessage($"  Median: {GetPercentile(latencies, 50):F2}");
            LogMessage($"  95th: {GetPercentile(latencies, 95):F2}");
            LogMessage($"  99th: {GetPercentile(latencies, 99):F2}");

            SaveResultsToCsv();
        }

        private static double GetPercentile(double[] data, int percentile)
        {
            if (data.Length == 0) return 0;
            double index = (percentile / 100.0) * (data.Length - 1);
            return data[(int)index];
        }

        private static void SaveResultsToCsv()
        {
            string fileName = $"redpanda-results-{DateTime.Now:yyyyMMdd-HHmmss}.csv";
            using (var writer = new StreamWriter(fileName))
            {
                writer.WriteLine("MessageId,LatencyMs,SentTimestamp,ReceivedTimestamp");
                foreach (var m in Latencies.OrderBy(m => m.MessageId))
                {
                    writer.WriteLine($"{m.MessageId},{m.LatencyMs:F3},{m.SentTimestamp},{m.ReceivedTimestamp}");
                }
            }
            LogMessage($"Results saved to {fileName}");
        }

        private static void LogMessage(string message)
        {
            string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {message}";
            Console.WriteLine(logEntry);
            File.AppendAllText(_logFile, logEntry + Environment.NewLine);
        }
    }

    class LatencyMeasurement
    {
        public int MessageId { get; set; }
        public double LatencyMs { get; set; }
        public long SentTimestamp { get; set; }
        public long ReceivedTimestamp { get; set; }
    }
}