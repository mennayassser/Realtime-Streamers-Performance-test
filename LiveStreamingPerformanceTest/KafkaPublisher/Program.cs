using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Text;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace KafkaPublisher
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long StartTimestamp { get; set; }
        public string Content { get; set; }
    }

    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string KAFKA_TOPIC = "performance-test";
        private const string KAFKA_BROKER_HOST = "localhost";
        private const int KAFKA_BROKER_PORT = 9092;
        private static string logFile = $"kafka-publisher-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static void Main(string[] args)
        {
            LogMessage("Kafka Publisher Starting...");

            Console.WriteLine("Make sure Kafka is running and Kafka Consumer is ready.");
            Console.WriteLine("Press any key to start the performance test...");
            Console.ReadKey();

            try
            {
                RunPerformanceTest().Wait();
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
            var options = new KafkaOptions(new Uri($"http://{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"))
            {
                Log = new ConsoleLog(),
                ResponseTimeoutMs = TimeSpan.FromSeconds(30),
            };

            var router = new BrokerRouter(options);
            var client = new Producer(router);

            try
            {
                LogMessage("Connected to Kafka broker");
                await Task.Delay(1000);

                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages");
                await SendMessages(client, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up phase completed");

                await Task.Delay(2000);

                LogMessage($"Starting performance test with {TEST_MESSAGES} messages");
                var stopwatch = Stopwatch.StartNew();

                await SendMessages(client, TEST_MESSAGES, "TEST");

                stopwatch.Stop();
                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;

                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms");
                LogMessage($"Throughput: {throughput:F2} messages/second");

                LogMessage("Waiting for consumer to process all messages...");
                await Task.Delay(5000);
            }
            finally
            {
                client.Dispose();
                router.Dispose();
            }
        }

        private static async Task SendMessages(Producer client, int messageCount, string phase)
        {
            for (int i = 1; i <= messageCount; i++)
            {
                var message = new Message
                {
                    Id = i,
                    Phase = phase,
                    StartTimestamp = DateTime.UtcNow.Ticks,
                    Content = $"Message {i} from Kafka Publisher"
                };

                try
                {
                    var json = JsonConvert.SerializeObject(message);
                    var kafkaMessage = new KafkaNet.Protocol.Message(json);

                    await client.SendMessageAsync(KAFKA_TOPIC, new[] { kafkaMessage });

                    if (i % 100 == 0)
                    {
                        await Task.Delay(10);
                        LogMessage($"Sent {i} {phase} messages...");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to send message {message.Id}: {ex.Message}");
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