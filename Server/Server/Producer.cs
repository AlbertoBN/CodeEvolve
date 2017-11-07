using Messages;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{

    public class DataEventArgs
    {
        public DataEventArgs(DataMessage data) { Data = data; }
        public DataMessage Data { get; private set; }
    }
    
    public class Producer
    {
        private CancellationTokenSource _src;

        // Declare the delegate (if using non-generic pattern).
        public delegate void DataEventHandler(object sender, DataEventArgs e);

        // Declare the event.
        public event DataEventHandler DataEvent;


        protected virtual void RaiseDataEvent(DataMessage data)
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

                while (true)
                {
                    if (_src.Token.IsCancellationRequested)
                        return;

                    DataMessage msg = new DataMessage();
                    msg.MessageId = Guid.NewGuid();
                    msg.MessageData = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut ";
                    msg.MessageNumber = rand.Next(int.MinValue, int.MaxValue);

                    RaiseDataEvent(msg);

                    Thread.Sleep(1000);
                }
            }, _src.Token);
        }

        internal void StopProducing()
        {
            _src.Cancel();
        }
    }
}
