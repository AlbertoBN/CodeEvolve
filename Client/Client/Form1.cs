using System;
using System.Windows.Forms;
using System.Collections.Generic;
using Messages;

namespace Client
{
    public partial class Form1 : Form
    {
        private object _locker;
        private List<DataMessage> _dataMessageList;

        public Form1()
        {
            InitializeComponent();
            Load += Form1_Load;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
        }

        public void PrintMessage(string msg)
        {
            this.Invoke((MethodInvoker)(() =>
            {
                _dataDisplay.Items.Add(msg);

            }));

        }
    }
}

