using System;
using System.Windows.Forms;
using Server;
using System.Collections.Generic;
using System.Threading;
using Messages;
using System.Linq;
using System.Diagnostics;

namespace Client
{
    public partial class Form1 : Form
    {        
        private object _locker;
        private List<DataMessage> _dataMessageList;
        private Thread _consumeThread;
        
        public Form1()
        {
            InitializeComponent();
            Load += Form1_Load;
            _locker = new object();
            _dataMessageList = new List<DataMessage>();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            _consumeThread = new Thread(ProcessDataMessages);
            _consumeThread.Start();
        }

        private void ProcessDataMessages()
        {
            while (true)
            {
                List<DataMessage> tmpList;
                lock (_dataMessageList)
                {
                    tmpList = _dataMessageList.ToList();
                    _dataMessageList.Clear();
                }

                foreach(DataMessage msg in tmpList)
                {
                    //simulating processing time before showing data
                   Thread.Sleep(1000);
                   this.Invoke((MethodInvoker)(() =>
                   {
                     
                       _dataDisplay.Items.Add($"{msg.MessageId} - {msg.MessageTime.ToShortTimeString()}");

                   }));
                    
                }
                tmpList.Clear();
            }
        }

        public void ConsumeData(object sender, DataEventArgs e)
        {
            lock (_dataMessageList)
            {
                _dataMessageList.AddRange(e.Data.ToList());
                e.Data.Clear();
               
            }
        }
    }
}
