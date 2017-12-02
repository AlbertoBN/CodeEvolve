using System;
using System.Collections.Generic;

namespace Server
{
    class Program
    {
        static void Main(string[] args)
        {
            List<Producer> producers = new List<Producer>();

            for (int i = 0; i < 10; i++)
            {
                Producer p = new Producer();
                p.Produce(i);
                producers.Add(p);
            }

            Console.WriteLine("Press any key to stop producing");
            Console.ReadKey();

            foreach(var p in producers)
            {
                p.StopProducing();
            }
        }
    }
}
