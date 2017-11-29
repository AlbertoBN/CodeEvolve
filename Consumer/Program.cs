using System;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Consumer consumer = new Consumer();
            consumer.Consume();

            Console.WriteLine("Press Any Key to stop consuming");
            Console.ReadKey();

            consumer.HaltConsumer();
        }
    }
}
