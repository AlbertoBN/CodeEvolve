using Messages;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Consumer
{
    public class Consumer
    {
        CancellationTokenSource _cancellationTokenSource;
    
     
        public void Consume()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING", EnvironmentVariableTarget.Machine);

            int readMessagePerProducer = Int32.Parse(ConfigurationManager.AppSettings["ReadMessagesPerProducer"]);
            int numberOfProducers = Int32.Parse(ConfigurationManager.AppSettings["NumberOfProducers"]);
            
            Task t = Task.Factory.StartNew(() =>
            {
                ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConnectionString);
                IDatabase db = redis.GetDatabase();

                //Cleanup old messages. We need them not
                for (int i = 0; i < numberOfProducers; i++)
                {
                    db.KeyDelete($"Producer_{i}");
                }

                ConcurrentDictionary<DateTime, List<DataMessage>> groupedMessages = new ConcurrentDictionary<DateTime, List<DataMessage>>();

                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    List<RedisValue> producerData = new List<RedisValue>();
                    List<DateTime> keysToClean = new List<DateTime>();

                    for (int i = 0; i < numberOfProducers; i++)
                    {
                        RedisValue[] values = db.ListRange($"Producer_{i}", 0, readMessagePerProducer - 1, CommandFlags.HighPriority);
                        db.ListTrim($"Producer_{i}", 0, readMessagePerProducer - 1, CommandFlags.HighPriority);
                        if (values != null && values.Length > 0)
                        {
                            producerData.AddRange(values);
                        }
                    }
                   
                    Parallel.ForEach(producerData, (value) =>
                    {
                        DataMessage dataMessage = JsonConvert.DeserializeObject<DataMessage>(value.ToString());

                        groupedMessages.AddOrUpdate(dataMessage.MessageTime, new List<DataMessage>() { dataMessage }, (k, v) =>
                        {
                            lock (v) //lock needed beause List<> is not synchronized. A ConcurrentBag may have done it but it is even more complex
                            {
                                v.Add(dataMessage);
                                return v;
                            }
                        });
                    });

                    producerData.Clear();
                    DateTime anchor = DateTime.Now;

                    foreach (var grp in groupedMessages.Keys)
                    {
                        if((anchor-grp).TotalSeconds>60)
                        {
                            keysToClean.Add(grp);
                            continue;
                        }

                        var dataMessage = groupedMessages[grp].First();
                        string msg = $"{groupedMessages[grp].Count} total messages for {dataMessage.MessageTime.ToString("HH:mm:ss.fff")}";
                        db.Publish("GroupedDataMessages", msg, CommandFlags.FireAndForget);
                    }
                    
                    //Messages Cleanup
                    foreach(var key in keysToClean)
                    {
                        List<DataMessage> msgList = new List<DataMessage>();
                        groupedMessages.TryRemove(key, out msgList);
                        msgList.Clear();
                    }

                    keysToClean.Clear();
                    Thread.Sleep(1000);//long processing
                }
            }, _cancellationTokenSource.Token);
        }

        public void HaltConsumer()
        {
            _cancellationTokenSource.Cancel();
        }
    }
}
