using System;
using System.Configuration;

namespace Client
{
    public static class SystemSettings
    {
        public static int NumberOfProducers
        {
            get
            {
                return Int32.Parse(ConfigurationManager.AppSettings["NumberOfProducers"]);
            }
        }

        public static int BatchBlockBufferSize
        {
            get
            {
                return Int32.Parse(ConfigurationManager.AppSettings["BatchBlockBufferSize"]);
            }
        }

        public static int MaxDegreeOfParallelism
        {
            get
            {
                return Int32.Parse(ConfigurationManager.AppSettings["MaxDegreeOfParallelism"]);
            }
        }

        
        public static int ProducerPauseInMillis
        {
            get
            {
                return Int32.Parse(ConfigurationManager.AppSettings["ProducerPauseInMillis"]);
            }
        }

    }
}
