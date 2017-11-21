using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Messages
{
    [Serializable]
    public class DataMessage
    {
        public Guid MessageId{ get; set;} 
        public string MessageData { get; set; }
        public int MessageNumber { get; set; }
        public DateTime MessageTime { get; set; }
        public Guid ProducerId { get; set; }
        
        public override string ToString()
        {
            return $"{MessageId} - {MessageTime} - {ProducerId}";
        }
    }
}
