using AMCDS.Protos;
using AMCDS.Services;

namespace AMCDS.Models
{
    public class EventualLeaderDetector : Abstraction
    {
        public static readonly string name = "eld";

        private ProcessId Leader { get; set; }
        private HashSet<ProcessId> Suspected { get; set; }

        public EventualLeaderDetector() { }

        public EventualLeaderDetector(string id, System system) : base(id, system)
        {
            SystemService.AddAbstraction(system,
                new EventuallyPerfectFailureDetector(MiscService.GetAbstractionChild(id, EventuallyPerfectFailureDetector.name), system));

            Suspected = new HashSet<ProcessId>();
        }

        public override bool Handle(Message message)
        {
            Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

            if (message.Type == Message.Types.Type.EpfdSuspect)
            {
                HandleEpdfSuspect(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.EpfdRestore)
            {
                HandleEpdfRestore(message);
                return true;
            }

            return false;
        }

        private void HandleEpdfSuspect(Message message)
        {
            Suspected.Add(message.EpfdSuspect.Process);
            HandleLeaderChange();
        }

        private void HandleEpdfRestore(Message message)
        {
            Suspected.Remove(message.EpfdRestore.Process);
            HandleLeaderChange();
        }

        private void HandleLeaderChange()
        {
            var aliveProcesses = System.Processes.Except(Suspected);
            var proposedLeader = aliveProcesses.OrderByDescending(x => x.Rank).FirstOrDefault();
            if (proposedLeader == null) return;

            if (!proposedLeader.Equals(Leader))
            {
                Leader = proposedLeader;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.EldTrust,
                    EldTrust = new EldTrust
                    {
                        Process = Leader
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
