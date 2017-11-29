using Messages;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

            Task t = Task.Factory.StartNew(() =>
            {
                ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConnectionString);
                IDatabase db = redis.GetDatabase();
                BatchBlock<DataMessage> batchblock = CreateDataFlowComponentsChain(db);

                while (true)
                {
                    if (_cancellationTokenSource.Token.IsCancellationRequested)
                        return;
                    
                    RedisValue value = db.ListRightPop("DataMessages");
                    if (value.HasValue)
                    {
                        DataMessage dataMessage = JsonConvert.DeserializeObject<DataMessage>((string)value);
                        batchblock.Post(dataMessage);//Good old Dataflow to the rescue
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }
                }
            }, _cancellationTokenSource.Token);
        }

        public void HaltConsumer()
        {
            _cancellationTokenSource.Cancel();
        }

        private static BatchBlock<DataMessage> CreateDataFlowComponentsChain(IDatabase redisDb)
        {
            //Build the blocks for the pipeline

            //First a batch block to receive and push producer data
            var dataPusher = new BatchBlock<DataMessage>(10);

            ConcurrentDictionary<string, List<DataMessage>> groupedMessages = new ConcurrentDictionary<string, List<DataMessage>>();
            ExecutionDataflowBlockOptions exdbo = new ExecutionDataflowBlockOptions();
            exdbo.MaxDegreeOfParallelism = 1;

            var dataGrouper = new TransformBlock<DataMessage[], List<List<DataMessage>>>(msgList =>
            {
                Parallel.ForEach(msgList, (msg) =>
                {
                    groupedMessages.AddOrUpdate(msg.MessageTime.ToShortTimeString(), new List<DataMessage>() { msg }, (k, v) => { v.Add(msg); return v; });
                });
                var returnData = groupedMessages.Values.ToList();
                return returnData;
            });

            var finalProcessor = new ActionBlock<List<List<DataMessage>>>(msgLists =>
            {


                foreach (var grp in msgLists)
                {
                    Thread.Sleep(1000);
                    var dataMessage = grp.First();

                    string msg = $"{grp.Count} total messages for {dataMessage.MessageTime}";
                    redisDb.Publish("GroupedDataMessages", msg, CommandFlags.FireAndForget);

                }
                msgLists.Clear();

            }, exdbo);

            dataPusher.LinkTo(dataGrouper);
            dataGrouper.LinkTo(finalProcessor);

            return dataPusher;
        }
    }
}
