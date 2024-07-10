using AMCDS.Protos;
using AMCDS.Services;

namespace AMCDS.Models
{
    public class BestEffortBroadcast : Abstraction
    {
        public static readonly string name = "beb";

        public BestEffortBroadcast() { }

        public BestEffortBroadcast(string id, System system) : base(id, system) 
        {
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));
        }

        public override bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.BebBroadcast)
            {
                HandleBebBroadcast(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.PlDeliver)
            {
                HandleBebDeliver(message);
                return true;
            }

            return false;
        }

        private void HandleBebBroadcast(Message message)
        {
            foreach (var process in System.Processes)
            {
                var finalMessage = new Message
                {
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Destination = process,
                        Message = message.BebBroadcast.Message
                    },
                    SystemId = System.SystemId,
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }

        private void HandleBebDeliver(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.BebDeliver,
                BebDeliver = new BebDeliver
                {
                    Message = message.PlDeliver.Message,
                    Sender = message.PlDeliver.Sender
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
