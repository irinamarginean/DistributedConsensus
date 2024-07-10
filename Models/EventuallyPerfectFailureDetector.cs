using AMCDS.Protos;
using AMCDS.Services;
using System.Timers;
using Timer = System.Timers.Timer;

namespace AMCDS.Models
{
    public class EventuallyPerfectFailureDetector : Abstraction
    {
        public static readonly string name = "epfd";

        private HashSet<ProcessId> Alive { get; set; }
        private HashSet<ProcessId> Suspected { get; set; }
        private readonly int delta = 200;
        private int delay;
        private Timer timer;

        public EventuallyPerfectFailureDetector() { }

        public EventuallyPerfectFailureDetector(string id, System system) : base(id, system)
        {
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));

            Alive = new HashSet<ProcessId>(System.Processes);
            Suspected = new HashSet<ProcessId>();
            delay = delta;

            timer = new Timer();
            timer.AutoReset = false;
            timer.Elapsed += new ElapsedEventHandler((source, e) =>
            {
                var msg = new Message
                {
                    Type = Message.Types.Type.EpfdTimeout,
                    EpfdTimeout = new EpfdTimeout(),
                    SystemId = System.SystemId,
                    ToAbstractionId = Id,
                    FromAbstractionId = Id,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(msg);
            });

            SystemService.AddTimer(System, timer);
            System.AreTimersActive = true;

            StartTimer();
        }

        public override bool Handle(Message message)
        {
            if (!System.AreTimersActive)
            {
                Console.WriteLine("Timer stopped for process " + System.ProcessId.Owner + "-" + System.ProcessId.Index);

                return true;
            }

            Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

            if (message.Type == Message.Types.Type.EpfdTimeout)
            {
                HandleEpfdTimeout(message);

                return true;
            }
            else if (message.Type == Message.Types.Type.PlDeliver)
            {
                if (message.PlDeliver.Message.Type == Message.Types.Type.EpfdInternalHeartbeatRequest)
                {
                    HandleEpdfInternalHeartbeatRequest(message);

                    return true;
                }
                else if (message.PlDeliver.Message.Type == Message.Types.Type.EpfdInternalHeartbeatReply)
                {
                    HandleEpdfInternalHeartbeatReply(message);

                    return true;
                }
            }

            return false;
        }

        private void HandleEpfdTimeout(Message message)
        {
            if (Alive.Intersect(Suspected).Count() != 0)
            {
                delay += delta;
                Console.WriteLine($"***********{System.ProcessId.Owner}-{System.ProcessId.Index} => Increased timeout to {delay}");
            }

            foreach (var processId in System.Processes)
            {
                if (!Alive.Contains(processId) && !Suspected.Contains(processId))
                {
                    Suspected.Add(processId);

                    var msg = new Message
                    {
                        Type = Message.Types.Type.EpfdSuspect,
                        EpfdSuspect = new EpfdSuspect
                        {
                            Process = processId
                        },
                        FromAbstractionId = Id,
                        ToAbstractionId = MiscService.GetAbstractionParent(Id),
                        SystemId = System.SystemId,
                        MessageUuid = Guid.NewGuid().ToString()
                    };

                    System.MessageQueue.Add(msg);
                }
                else if (Alive.Contains(processId) && Suspected.Contains(processId))
                {
                    Suspected.Remove(processId);

                    var msg = new Message
                    {
                        Type = Message.Types.Type.EpfdRestore,
                        EpfdRestore = new EpfdRestore
                        {
                            Process = processId
                        },
                        FromAbstractionId = Id,
                        ToAbstractionId = MiscService.GetAbstractionParent(Id),
                        SystemId = System.SystemId,
                        MessageUuid = Guid.NewGuid().ToString()
                    };

                    System.MessageQueue.Add(msg);
                }

                var finalMesage = new Message
                {
                    Type = Message.Types.Type.PlSend,
                    PlSend = new PlSend
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EpfdInternalHeartbeatRequest,
                            EpfdInternalHeartbeatRequest = new EpfdInternalHeartbeatRequest(),
                            FromAbstractionId = Id,
                            ToAbstractionId = Id,
                            SystemId = System.SystemId,
                            MessageUuid = Guid.NewGuid().ToString()
                        },
                        Destination = processId
                    },
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                    SystemId = System.SystemId,
                    MessageUuid = Guid.NewGuid().ToString()
                };

                System.MessageQueue.Add(finalMesage);
            }

            Alive.Clear();
            StartTimer();
        }

        private void HandleEpdfInternalHeartbeatRequest(Message message)
        {
            var finalMesage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.EpfdInternalHeartbeatReply,
                        EpfdInternalHeartbeatReply = new EpfdInternalHeartbeatReply(),
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        SystemId = System.SystemId,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = message.PlDeliver.Sender
                },
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                SystemId = System.SystemId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMesage);
        }

        private void HandleEpdfInternalHeartbeatReply(Message message)
        {
            Alive.Add(message.PlDeliver.Sender);
        }

        private void StartTimer()
        {
            timer.Interval = delay;
            timer.Start();
        }
    }
}
