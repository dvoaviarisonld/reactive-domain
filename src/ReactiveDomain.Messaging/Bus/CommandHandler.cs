using System;
using ReactiveDomain.Logging;
using ReactiveDomain.Util;

namespace ReactiveDomain.Messaging.Bus
{
    public class CommandHandler<T> : IHandle<T> where T : Command
    {
        // ReSharper disable once StaticMemberInGenericType
        private static readonly ILogger Log = LogManager.GetLogger("ReactiveDomain");
        private readonly IPublisher _bus;
        private readonly IHandleCommand<T> _handler;
        public CommandHandler(IPublisher returnBus, IHandleCommand<T> handler)
        {
            Ensure.NotNull(returnBus, "returnBus");
            Ensure.NotNull(handler, "handler");
            _bus = returnBus;
            _handler = handler;
        }

        public void Handle(T message)
        {
            _bus.Publish(new AckCommand(message));
            try
            {
                if (Log.LogLevel >= LogLevel.Debug)
                    Log.Debug("{0} command handled by {1}", message.GetType().Name, _handler.GetType().Name);
                _bus.Publish(_handler.Handle(message));
            }
            catch (Exception ex)
            {
                _bus.Publish(message.Fail(ex));
            }
        }
    }
}