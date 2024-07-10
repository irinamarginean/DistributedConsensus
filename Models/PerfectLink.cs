using AMCDS.Protos;
using AMCDS.Services;
using Google.Protobuf;

namespace AMCDS.Models
{
    public class PerfectLink : Abstraction
    {
        public static readonly string name = "pl";

        public PerfectLink() { }

        public PerfectLink(string id, System system) : base(id, system) { }

        public override bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.NetworkMessage)
            {
                HandlePlDeliver(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.PlSend)
            {
                HandlePlSend(message);
                return true;
            }

            return true;
        }

        private void HandlePlSend(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.NetworkMessage,
                NetworkMessage = new NetworkMessage
                {
                    Message = message.PlSend.Message,
                    SenderHost = System.ProcessId.Host,
                    SenderListeningPort = System.ProcessId.Port,
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = message.ToAbstractionId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            var data = finalMessage.ToByteArray();
            var ipAddress = message.PlSend.Destination.Host;
            var port = message.PlSend.Destination.Port;

            TcpService.Send(data, ipAddress, port);
        }

        private void HandlePlDeliver(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlDeliver,
                PlDeliver = new PlDeliver
                {
                    Message = message.NetworkMessage.Message,
                    Sender = System.Processes.FirstOrDefault(x =>
                        x.Host == message.NetworkMessage.SenderHost && x.Port == message.NetworkMessage.SenderListeningPort)
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionParent(Id), 
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }
    }
}
