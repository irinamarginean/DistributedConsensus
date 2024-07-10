using AMCDS.Models;
using AMCDS.Protos;
using Google.Protobuf;
using System.Net;
using Timer = System.Timers.Timer;

namespace AMCDS.Services
{
    public static class SystemService
    {
        public static void Run(Models.System system, CancellationTokenSource cancellationTokenSource)
        {
            RegisterToHub(system);

            Task.Run(() =>
            {
                var message = new Message();
                var processHost = IPAddress.Parse(system.ProcessId.Host);
                var processPort = system.ProcessId.Port;

                TcpService.Listen(cancellationTokenSource, processHost, processPort, (message) =>
                {
                    if (message.NetworkMessage.Message.Type == Message.Types.Type.ProcInitializeSystem)
                    {
                        HandleProcInit(system, message.NetworkMessage.Message);
                    }
                    else if (message.NetworkMessage.Message.Type == Message.Types.Type.ProcDestroySystem)
                    {
                        HandleProcDestroy(system);
                    }
                    else
                    {
                        system.MessageQueue.Add(message);
                    }
                });
            });

            foreach (var message in system.MessageQueue.GetConsumingEnumerable())
            {
                HandleMessage(system, message);
            }
        }

        public static void AddAbstraction(Models.System system, Abstraction abstraction)
        {
            system.Abstractions.TryAdd(abstraction.Id, abstraction);
            Console.WriteLine($"{system.ProcessId.Owner}-{system.ProcessId.Index} => registered {abstraction.Id}");
        }

        public static void AddTimer(Models.System system, Timer timer)
        {
            system.Timers.Add(timer);
        }

        public static void StopTimers(Models.System system)
        {
            system.Timers.ForEach(x => x.Stop());
            system.AreTimersActive = false;
        }

        private static void RegisterToHub(Models.System system)
        {
            var message = new Message
            {
                Type = Message.Types.Type.ProcRegistration,
                ToAbstractionId = system.HubProcessId.Owner,
                MessageUuid = Guid.NewGuid().ToString(),
                ProcRegistration = new ProcRegistration
                {
                    Owner = system.ProcessId.Owner,
                    Index = system.ProcessId.Index
                }
            };

            var networkMessage = new Message
            {
                Type = Message.Types.Type.NetworkMessage,
                ToAbstractionId = message.ToAbstractionId,
                MessageUuid = Guid.NewGuid().ToString(),
                SystemId = message.SystemId,
                NetworkMessage = new NetworkMessage
                {
                    Message = message,
                    SenderHost = system.ProcessId.Host,
                    SenderListeningPort = system.ProcessId.Port
                }
            };

            var data = networkMessage.ToByteArray();

            TcpService.Send(data, system.HubProcessId.Host, system.HubProcessId.Port);
        }

        private static void HandleProcInit(Models.System system, Message message)
        {
            while (system.MessageQueue.TryTake(out _)) { }

            var application = new Application(Application.name, system);

            AddAbstraction(system, application);

            foreach (var process in message.ProcInitializeSystem.Processes)
            {
                system.Processes.Add(process);
            }

            system.ProcessId = system.Processes.FirstOrDefault(x => x.Host == system.ProcessId.Host && x.Port == system.ProcessId.Port);

            system.SystemId = message.SystemId;

        }

        private static void HandleProcDestroy(Models.System system)
        {
            system.Processes.Clear();
            system.Abstractions.Clear();
        }

        private static void HandleMessage(Models.System system, Message message)
        {
            if (system.Abstractions.ContainsKey(message.ToAbstractionId))
            {
                if (!system.Abstractions[message.ToAbstractionId].Handle(message))
                {
                    system.MessageQueue.Add(message);
                }
            }
            else
            {
                Console.WriteLine("Unknown abstraction " + system.ProcessId.Owner + " " + system.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

                if (message.ToAbstractionId.Contains(NNAtomicRegister.name))
                {
                    var unknownRegisterName = MiscService.GetRegisterName(message.ToAbstractionId);
                    var unknownAbstractionId = MiscService.GetNNarAbstractionId(Application.name, unknownRegisterName);
                    AddAbstraction(system, new NNAtomicRegister(unknownAbstractionId, system));
                    system.Abstractions[message.ToAbstractionId].Handle(message);

                    return;
                }

                system.MessageQueue.Add(message);
            }
        }
    }
}
