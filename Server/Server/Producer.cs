using Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{

    public class DataEventArgs
    {
        public DataEventArgs(List<DataMessage> data) { Data = data; }
        public List<DataMessage> Data { get; private set; }
    }
    
    public class Producer
    {
        private CancellationTokenSource _src;

        // Declare the delegate (if using non-generic pattern).
        public delegate void DataEventHandler(object sender, DataEventArgs e);

        // Declare the event.
        public event DataEventHandler DataEvent;


        protected virtual void RaiseDataEvent(List<DataMessage> data)
        {
            // Raise the event by using the () operator.
            if (DataEvent != null)
            {
                DataEvent(this, new DataEventArgs(data));
            }
        }

        public void Produce()
        {
            _src = new CancellationTokenSource();

            Task t = Task.Factory.StartNew(() =>
            {
                Random rand = new Random((int)DateTime.Now.Ticks);

                
                while (!_src.Token.IsCancellationRequested)
                {

                    List<DataMessage> dataMessages = new List<DataMessage>();
                    for (int i = 0; i < 100; i++)
                    {
                        DataMessage msg = new DataMessage();
                        msg.MessageId = Guid.NewGuid();
                        msg.MessageData = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut ";
                        msg.MessageNumber = rand.Next(int.MinValue, int.MaxValue);
                        msg.MessageTime = DateTime.Now;
                        dataMessages.Add(msg);
                    }

                    RaiseDataEvent(dataMessages);
                    dataMessages.Clear();
                }
            }, _src.Token);
        }

        internal void StopProducing()
        {
            _src.Cancel();
        }
    }
}
