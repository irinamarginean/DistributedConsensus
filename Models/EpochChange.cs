using AMCDS.Protos;
using AMCDS.Services;

namespace AMCDS.Models
{
    public class EpochChange : Abstraction
    {
        public static readonly string name = "ec";

        private ProcessId trusted;
        private int lastTimestamp;
        private int timestamp;

        public EpochChange() { }
        public EpochChange(string id, System system): base(id, system) 
        {
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));
            SystemService.AddAbstraction(system, new BestEffortBroadcast(MiscService.GetAbstractionChild(id, BestEffortBroadcast.name), system));
            SystemService.AddAbstraction(system, new EventualLeaderDetector(MiscService.GetAbstractionChild(id, EventualLeaderDetector.name), system));

            trusted = System.Processes.OrderByDescending(x => x.Rank).FirstOrDefault();
            lastTimestamp = 0;
            timestamp = System.ProcessId.Rank;
        }

        public override bool Handle(Message message)
        {
            Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

            if (message.Type == Message.Types.Type.EldTrust)
            {
                HandleEldTrust(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.BebDeliver)
            {
                if (message.BebDeliver.Message.Type == Message.Types.Type.EcInternalNewEpoch)
                {
                    HandleInternalNewEpoch(message);
                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.PlDeliver)
            {
                if (message.PlDeliver.Message.Type == Message.Types.Type.EcInternalNack)
                {
                    HandleInternalNack(message);
                    return true;
                }
            }
           
            return false;
        }

        private void HandleEldTrust(Message message)
        {
            trusted = message.EldTrust.Process;

            if (trusted.Equals(System.ProcessId))
            {
                timestamp += System.Processes.Count;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EcInternalNewEpoch,
                            EcInternalNewEpoch = new EcInternalNewEpoch
                            {
                                Timestamp = timestamp
                            },
                            FromAbstractionId = Id,
                            ToAbstractionId = Id,
                            SystemId = System.SystemId,
                            MessageUuid = Guid.NewGuid().ToString()
                        }
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionChild(Id, BestEffortBroadcast.name),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }

        private void HandleInternalNewEpoch(Message message)
        {
            if (message.BebDeliver.Sender.Equals(trusted) && message.BebDeliver.Message.EcInternalNewEpoch.Timestamp > lastTimestamp)
            {
                lastTimestamp = message.BebDeliver.Message.EcInternalNewEpoch.Timestamp;

                Console.WriteLine($"start new epoch {message.BebDeliver.Message.EcInternalNewEpoch.Timestamp}");

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.EcStartEpoch,
                    EcStartEpoch = new EcStartEpoch
                    {
                        NewTimestamp = message.BebDeliver.Message.EcInternalNewEpoch.Timestamp,
                        NewLeader = message.BebDeliver.Sender
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionParent(Id),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
            else
            {
                var finalMessage = new Message
                {
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EcInternalNack,
                            EcInternalNack = new EcInternalNack(),
                            FromAbstractionId = Id,
                            ToAbstractionId = Id,
                            SystemId = System.SystemId,
                            MessageUuid = Guid.NewGuid().ToString()
                        },
                        Destination = message.BebDeliver.Sender
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }

        private void HandleInternalNack(Message message)
        {
            if (trusted.Equals(System.ProcessId))
            {
                timestamp += System.Processes.Count;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EcInternalNewEpoch,
                            EcInternalNewEpoch = new EcInternalNewEpoch
                            {
                                Timestamp = timestamp
                            },
                            FromAbstractionId = Id,
                            ToAbstractionId = Id,
                            SystemId = System.SystemId,
                            MessageUuid = Guid.NewGuid().ToString()
                        }
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionChild(Id, BestEffortBroadcast.name),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }
    }
}
