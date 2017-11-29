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

        public void Produce()
        {
            _src = new CancellationTokenSource();
            Guid producerId = Guid.NewGuid();
            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING", EnvironmentVariableTarget.Machine);

            int prpducerPauseInMillies = Int32.Parse(ConfigurationManager.AppSettings["ProducerPauseInMillis"]);

            Task t = Task.Factory.StartNew(() =>
            {
                Random rand = new Random((int)DateTime.Now.Ticks);
                ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisConnectionString);
                IDatabase db = redis.GetDatabase();
                while (!_src.Token.IsCancellationRequested)
                {
                    //Now we just push messages in. The pipeline will deal with the numbers
                    DataMessage msg = new DataMessage();
                    msg.MessageId = Guid.NewGuid();
                    msg.MessageData = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut ";
                    msg.MessageNumber = rand.Next(int.MinValue, int.MaxValue);
                    msg.MessageTime = DateTime.Now;
                    msg.ProducerId = producerId;

                    string jsonMessage = JsonConvert.SerializeObject(msg);

                    
                    db.ListLeftPush("DataMessages", jsonMessage, When.Always, CommandFlags.FireAndForget);
                    Thread.Sleep(Int32.Parse(ConfigurationManager.AppSettings["ProducerPauseInMillis"])); //preventing overflow
                }
            }, _src.Token);
        }

        internal void StopProducing()
        {
            _src.Cancel();
        }
    }
}
