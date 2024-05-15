using Shared.Events;

namespace Order.API.Services
{
    public static class BusExt
    {
        public static async Task CreateTopicsOrQueues(this WebApplication app)
        {
            using var scope = app.Services.CreateScope();
            var bus = scope.ServiceProvider.GetRequiredService<IBus>();

            await bus.CreateTopicOrQueue([BusConstants.OrderCreatedEventTopicName]);
        }
    }
}