using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Producer.Events
{
    internal record OrderCreatedEvent
    {
        public string OrderCode { get; init; } = default!;
        public decimal TotalPrice { get; init; }
        public int UserId { get; init; }
    }
}