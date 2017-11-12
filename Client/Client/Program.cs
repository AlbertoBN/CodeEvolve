using Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

            Form1 form = new Form1();
            _producers = new List<Producer>();
            for (int i = 0; i <= 1; i++)
            {
                Producer producer = new Producer();
                producer.DataEvent += form.ConsumeData;
                producer.Produce();
                _producers.Add(producer);
            }

            Application.Run(form);

        }
    }
}
