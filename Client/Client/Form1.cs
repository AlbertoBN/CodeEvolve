using System;
using System.Windows.Forms;
using Server;


namespace Client
{
    public partial class Form1 : Form
    {
        private Producer _producer;

        public Form1()
        {
            InitializeComponent();
            Load += Form1_Load; ;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            _producer = new Producer();
            _producer.DataEvent += ConsumeData;
            _producer.Produce();
        }

        private void ConsumeData(object sender, DataEventArgs e)
        {
            this.Invoke((MethodInvoker)(() => _dataDisplay.Items.Add(e.Data.MessageId)));
        }
    }
}
