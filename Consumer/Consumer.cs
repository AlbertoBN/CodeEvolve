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
                BatchBlock<DataMessage> batchblock = CreateDataFlowComponentsChain(db, readMessagePerProducer* numberOfProducers);

                //Cleanup old messages. We need them not
                for (int i = 0; i < numberOfProducers; i++)
                {
                    db.KeyDelete($"Producer_{i}");
                }

                while (true)
                {
                    if (_cancellationTokenSource.Token.IsCancellationRequested)
                        return;
                    
                    for(int i=0; i< numberOfProducers;i++)
                    {
                        RedisValue[] values = db.ListRange($"Producer_{i}",0, readMessagePerProducer-1,CommandFlags.HighPriority);
                        db.ListTrim($"Producer_{i}", 0, readMessagePerProducer - 1, CommandFlags.HighPriority);

                        if (values!=null && values.Length>0)
                        {
                            //believe it or not this code is blazing fact
                            foreach (RedisValue value in values)
                            {
                                DataMessage dataMessage = JsonConvert.DeserializeObject<DataMessage>(value.ToString());
                                batchblock.Post(dataMessage);
                            }
                        }
                    }
                    Thread.Sleep(10);
                }
            }, _cancellationTokenSource.Token);
        }

        public void HaltConsumer()
        {
            _cancellationTokenSource.Cancel();
        }

        private static BatchBlock<DataMessage> CreateDataFlowComponentsChain(IDatabase redisDb, int batchBlockSize)
        {
            //Build the blocks for the pipeline

            //First a batch block to receive and push producer data
            var dataPusher = new BatchBlock<DataMessage>(batchBlockSize);

            ConcurrentDictionary<string, List<DataMessage>> groupedMessages = new ConcurrentDictionary<string, List<DataMessage>>();
            var taskSchedulerPair = new ConcurrentExclusiveSchedulerPair();

          
            var dataGrouper = new TransformBlock<DataMessage[], List<List<DataMessage>>>(msgList =>
            {
                Parallel.ForEach(msgList, (msg) =>
                {
                    groupedMessages.AddOrUpdate(msg.MessageTime.ToString(), new List<DataMessage>() { msg }, (k, v) =>
                    {
                        lock (v) //lock needed becasue List<> is not synchronized. A ConcurrentBag may have done it but it is even more complex
                        {
                            v.Add(msg);
                            return v;
                        }
                    });
                });
                
                var returnData = groupedMessages.Values.ToList(); //copy data before cleatring
                groupedMessages.Clear();

                return returnData;

            }, new ExecutionDataflowBlockOptions
            {
                TaskScheduler = taskSchedulerPair.ConcurrentScheduler
            });

            var finalProcessor = new ActionBlock<List<List<DataMessage>>>(msgLists =>
            {
                foreach (var grp in msgLists)
                {
                    var dataMessage = grp.First();

                    string msg = $"{grp.Count} total messages for {dataMessage.MessageTime}";
                    redisDb.Publish("GroupedDataMessages", msg, CommandFlags.FireAndForget);
                    Thread.Sleep(1000);

                }

                msgLists.Clear();
                

            }, new ExecutionDataflowBlockOptions
            {
                TaskScheduler = taskSchedulerPair.ExclusiveScheduler
            });

            dataPusher.LinkTo(dataGrouper);
            dataGrouper.LinkTo(finalProcessor);

            return dataPusher;
        }
    }
}
