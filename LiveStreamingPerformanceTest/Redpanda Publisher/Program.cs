using System;
using System.IO;
using System.Text;
using System.Threading;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;

namespace RedPandaPublisher
{
    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"; // Updated port
        private const string TOPIC_NAME = "performance-test";
        private static string logFile = $"redpanda-publisher-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static void Main(string[] args)
        {
            LogMessage("RedPanda Publisher Starting...");
            Console.WriteLine("Make sure RedPanda is running. Press any key to start test...");
            Console.ReadKey();

            try
            {
                RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}\n{ex.StackTrace}");
            }

            Console.WriteLine("Test completed. Press any key to exit...");
            Console.ReadKey();
        }

        private static void RunPerformanceTest()
        {
            var options = new KafkaOptions(new Uri($"http://{KAFKA_BOOTSTRAP_SERVERS}"));
            var router = new BrokerRouter(options);
            var client = new Producer(router);

            try
            {
                LogMessage($"Connected to RedPanda at {KAFKA_BOOTSTRAP_SERVERS}");

                // Warm-up phase
                LogMessage($"Sending {WARM_UP_MESSAGES} warm-up messages");
                SendMessages(client, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up completed");

                Thread.Sleep(2000); // Let consumer catch up

                // Test phase
                LogMessage($"Sending {TEST_MESSAGES} test messages");
                var startTime = DateTime.UtcNow;
                SendMessages(client, TEST_MESSAGES, "TEST");
                var duration = (DateTime.UtcNow - startTime).TotalMilliseconds;

                LogMessage($"Test completed in {duration:F0} ms");
                LogMessage($"Throughput: {TEST_MESSAGES / (duration / 1000):F2} msg/sec");
            }
            finally
            {
                client.Dispose();
            }
        }

        private static void SendMessages(Producer client, int count, string phase)
        {
            for (int i = 1; i <= count; i++)
            {
                try
                {
                    // Create message with simplified structure
                    var message = new
                    {
                        Id = i,
                        Phase = phase,
                        Timestamp = DateTime.UtcNow.Ticks,
                        Content = $"Message {i} from Publisher"
                    };

                    var json = JsonConvert.SerializeObject(message);
                    var kafkaMessage = new Message(json);
                    client.SendMessageAsync(TOPIC_NAME, new[] { kafkaMessage }).Wait();

                    if (i % 500 == 0)
                        LogMessage($"Sent {i} {phase} messages");
                }
                catch (Exception ex)
                {
                    LogMessage($"Error sending message {i}: {ex.Message}");
                }
            }
            LogMessage($"Finished sending {count} {phase} messages");
        }

        private static void LogMessage(string message)
        {
            string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {message}";
            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }
}