using Messages;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Server
{
   
    public class Producer
    {
        private CancellationTokenSource _src;


        public void Produce(int queueId)
        {
            _src = new CancellationTokenSource();

            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING", EnvironmentVariableTarget.Machine);
            int messagesPerBatch = Int32.Parse(ConfigurationManager.AppSettings["MessagesPerBatch"]);
            int producerPauseInMillies = Int32.Parse(ConfigurationManager.AppSettings["ProducerPauseInMillis"]);
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConnectionString);

            Task t = Task.Factory.StartNew(() =>
            {
                Random rand = new Random((int)DateTime.Now.Ticks);
                
                IBatch batchOp = redis.GetDatabase().CreateBatch();
                Guid producerId = Guid.NewGuid();

                while (!_src.Token.IsCancellationRequested)
                {
                    for (int i = 0; i < messagesPerBatch; i++)
                    {
                        //Now we just push messages in. The pipeline will deal with the numbers
                        DataMessage msg = CreateMessage(producerId, rand.Next(int.MinValue, int.MaxValue));

                        string jsonMessage = JsonConvert.SerializeObject(msg);
                        batchOp.ListLeftPushAsync($"Producer_{queueId}", jsonMessage, When.Always, CommandFlags.FireAndForget);
                    }

                    batchOp.Execute();
                    Thread.Sleep(producerPauseInMillies); //preventing overflow
                }
            }, _src.Token);
        }

        private static DataMessage CreateMessage(Guid producerId, int messageNumber)
        {
            DataMessage msg = new DataMessage();
            msg.MessageId = Guid.NewGuid();
            msg.MessageData = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut ";
            msg.MessageNumber = messageNumber;
            msg.MessageTime = DateTime.Now;
            msg.ProducerId = producerId;
            return msg;
        }

        internal void StopProducing()
        {
            _src.Cancel();
        }
    }
}
