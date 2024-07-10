using AMCDS.Protos;
using AMCDS.Services;

namespace AMCDS.Models
{
    public class UniformConsensus : Abstraction
    {
        public static readonly string name = "uc";

        private Value value;
        private bool proposed;
        private bool decided;

        private int epochTimestamp = 0;
        private ProcessId leader;

        private int newEpochTimestamp = 0;
        private ProcessId newLeader;

        public UniformConsensus() { }

        public UniformConsensus(string id, System system) : base(id, system)
        {
            SystemService.AddAbstraction(system, new EpochChange(MiscService.GetAbstractionChild(Id, EpochChange.name), system));

            value = new Value { Defined = false };
            proposed = false;
            decided = false;
            leader = system.Processes.OrderByDescending(x => x.Rank).FirstOrDefault();

            var ep0State = new Tuple<int, Value>(0, new Value { Defined = false });

            SystemService.AddAbstraction(system,
               new EpochConsensus(MiscService.GetEpochConsensusAbstractionId(Id, epochTimestamp), system, ep0State, epochTimestamp, leader));
        }

        public override bool Handle(Message message)
        {
            Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

            if (message.Type == Message.Types.Type.UcPropose)
            {
                HandleUcPropose(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.EcStartEpoch)
            {
                HandleEcStartEpoch(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.EpAborted)
            {
                if (epochTimestamp == message.EpAborted.Ets)
                {
                    HandleEpAborted(message);
                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.EpDecide)
            {
                if (epochTimestamp == message.EpDecide.Ets)
                {
                    HandleEpDecide(message);
                    return true;
                }
            }

            return false;
        }

        private void HandleUcPropose(Message message)
        {
            value = message.UcPropose.Value;

            HandleProposalChange();
        }

        private void HandleProposalChange()
        {
            if (System.ProcessId.Equals(leader) && value.Defined && !proposed)
            {
                proposed = true;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.EpPropose,
                    EpPropose = new EpPropose
                    {
                        Value = value
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetEpochConsensusAbstractionId(Id, epochTimestamp),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }

        private void HandleEcStartEpoch(Message message)
        {
            newEpochTimestamp = message.EcStartEpoch.NewTimestamp;
            newLeader = message.EcStartEpoch.NewLeader;

            var finalMessage = new Message
            {
                Type = Message.Types.Type.EpAbort,
                EpAbort = new EpAbort(),
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetEpochConsensusAbstractionId(Id, epochTimestamp),
                SystemId = System.SystemId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleEpAborted(Message message)
        {
            epochTimestamp = newEpochTimestamp;
            leader = newLeader;
            proposed = false;

            var epState = new Tuple<int, Value>(message.EpAborted.ValueTimestamp, message.EpAborted.Value);

            SystemService.AddAbstraction(System,
                new EpochConsensus(MiscService.GetEpochConsensusAbstractionId(Id, epochTimestamp), System, epState, epochTimestamp, leader));

            HandleProposalChange();
        }

        private void HandleEpDecide(Message message)
        {
            if (!decided)
            {
                decided = true;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.UcDecide,
                    UcDecide = new UcDecide
                    {
                        Value = message.EpDecide.Value
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionParent(Id),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMessage);
            }
        }
    }
}
