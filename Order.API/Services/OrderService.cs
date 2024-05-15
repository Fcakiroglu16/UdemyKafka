using Order.API.Dtos;
using Shared.Events;
using Shared.Events.Events;

namespace Order.API.Services
{
    public class OrderService(IBus bus)
    {
        public async Task<bool> Create(OrderCreateRequestDto request)
        {
            var orderCode = Guid.NewGuid().ToString();

            // save to database
            var orderCreatedEvent = new OrderCreatedEvent(orderCode, request.UserId, request.TotalPrice);


            return await bus.Publish(orderCode, orderCreatedEvent, BusConstants.OrderCreatedEventTopicName);
        }
    }
}