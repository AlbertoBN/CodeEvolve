using Messages;
using Server;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Windows.Forms;

namespace Client
{
    static class Program
    {
        private static List<Producer> _producers;
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
        
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);


            //separated in function to have cleaner code
            Form1 form = new Form1();

            var dataFlowChain = CreateDataFlowComponentsChain( form);
            _producers = new List<Producer>();

            for (int i = 0; i <= 1; i++)
            {
                Producer producer = new Producer();
                
                producer.Produce(dataFlowChain);
                _producers.Add(producer);
            }

            Application.Run(form);

        }

        private static BatchBlock<DataMessage> CreateDataFlowComponentsChain(Form1 form) 
        {
            //Build the blocks for the pipeline

            //First a batch block to receive and push producer data
            var dataPusher = new BatchBlock<DataMessage>(10);

            ConcurrentDictionary<string, List<DataMessage>> groupedMessages = new ConcurrentDictionary<string, List<DataMessage>>();
            ExecutionDataflowBlockOptions exdbo = new ExecutionDataflowBlockOptions();
            exdbo.MaxDegreeOfParallelism = 1;
   
            var dataGrouper = new TransformBlock<DataMessage[], List<List<DataMessage>>>(msgList =>
            {
                foreach (var msg in msgList)
                {

                    if (groupedMessages.ContainsKey(msg.MessageTime.ToShortTimeString()))
                    {
                        lock (groupedMessages[msg.MessageTime.ToShortTimeString()])
                        {
                            groupedMessages[msg.MessageTime.ToShortTimeString()].Add(msg);
                        }
                    }
                    else
                    {
                        groupedMessages.TryAdd(msg.MessageTime.ToShortTimeString(), new List<DataMessage>() { msg });
                    }
                }
                
                var returnData = groupedMessages.Values.ToList();
                return returnData;

            });

            var finalProcessor = new ActionBlock<List<List<DataMessage>>>(msgLists => {

                
                foreach (var grp in msgLists)
                {
                    Thread.Sleep(1000);
                    var dataMessage = grp.First();
                    
                    string msg = $"{grp.Count} total messages for {dataMessage.MessageTime}";
                    form.PrintMessage(msg);
  
                }
                msgLists.Clear();
 
           }, exdbo);

            dataPusher.LinkTo(dataGrouper);
            dataGrouper.LinkTo(finalProcessor);
           
            return dataPusher;
        }

    }
}
