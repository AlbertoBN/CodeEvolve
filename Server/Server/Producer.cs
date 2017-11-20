using Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Server
{
   
    public class Producer
    {
        private CancellationTokenSource _src;

        public void Produce(BatchBlock<DataMessage> batchBlock)
        {
            _src = new CancellationTokenSource();

            Task t = Task.Factory.StartNew(() =>
            {
                Random rand = new Random((int)DateTime.Now.Ticks);


                while (!_src.Token.IsCancellationRequested)
                {
                    //Now we just push messages in. The pipeline will deal with the numbers
                    DataMessage msg = new DataMessage();
                    msg.MessageId = Guid.NewGuid();
                    msg.MessageData = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut ";
                    msg.MessageNumber = rand.Next(int.MinValue, int.MaxValue);
                    msg.MessageTime = DateTime.Now;

                    batchBlock.Post(msg);
                    Thread.Sleep(100); //preventing overflow
                }
            }, _src.Token);
        }

        internal void StopProducing()
        {
            _src.Cancel();
        }
    }
}
