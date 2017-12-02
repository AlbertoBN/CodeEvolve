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
            Guid producerId = Guid.NewGuid();
            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING", EnvironmentVariableTarget.Machine);

            int messagesPerBatch = Int32.Parse(ConfigurationManager.AppSettings["MessagesPerBatch"]);
            int producerPauseInMillies = Int32.Parse(ConfigurationManager.AppSettings["ProducerPauseInMillis"]);
            

            Task t = Task.Factory.StartNew(() =>
            {
                Random rand = new Random((int)DateTime.Now.Ticks);
                ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConnectionString);
                IDatabase db = redis.GetDatabase();
                int msgCounter = 0;
                IBatch batchOp = db.CreateBatch();
                while (!_src.Token.IsCancellationRequested)
                {
                    //Now we just push messages in. The pipeline will deal with the numbers
                    DataMessage msg = CreateMessage(producerId, rand.Next(int.MinValue, int.MaxValue));

                    string jsonMessage = JsonConvert.SerializeObject(msg);
                    batchOp.ListLeftPushAsync($"Producer_{queueId}", jsonMessage, When.Always, CommandFlags.FireAndForget);

                    msgCounter++;

                    if (msgCounter == messagesPerBatch)
                    {
                        msgCounter = 0;
                        batchOp.Execute();
                        
                        Thread.Sleep(producerPauseInMillies); //preventing overflow
                    }
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
