using AMCDS.Protos;
using AMCDS.Services;

namespace AMCDS.Models
{
    public class Application : Abstraction
    {
        public static readonly string name = "app";

        public Application() { }

        public Application(string id, System system) : base(id, system) 
        {
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));
            SystemService.AddAbstraction(system, new BestEffortBroadcast(MiscService.GetAbstractionChild(id, BestEffortBroadcast.name), system));
        }

        public override bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.PlDeliver)
            {
                var inMessage = message.PlDeliver.Message;

                if (inMessage.Type == Message.Types.Type.AppBroadcast)
                {
                    HandleAppBroadcast(inMessage);

                    return true;
                }
                else if (inMessage.Type == Message.Types.Type.AppWrite)
                {
                    HandleAppWrite(inMessage);

                    return true;
                }
                else if (inMessage.Type == Message.Types.Type.AppRead)
                {
                    HandleAppRead(inMessage);

                    return true;
                }
                else if (inMessage.Type == Message.Types.Type.AppPropose)
                {
                    Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

                    HandleAppPropose(inMessage);

                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.BebDeliver)
            {
                HandleBebDeliver(message);

                return true;
            }
            else if (message.Type == Message.Types.Type.NnarReadReturn)
            {
                HandleReadReturn(message);

                return true;
            }
            else if (message.Type == Message.Types.Type.NnarWriteReturn)
            {
                HandleWriteReturn(message);

                return true;
            }
            else if (message.Type == Message.Types.Type.UcDecide)
            {
                Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

                HandleUcDecide(message);

                return true;
            }

            return false;
        }

        private void HandleAppBroadcast(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.AppValue,
                        AppValue = new AppValue
                        {
                            Value = message.AppBroadcast.Value
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    }
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, BestEffortBroadcast.name),  
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleBebDeliver(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Destination = System.HubProcessId,
                    Message = message.BebDeliver.Message
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name), 
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleAppWrite(Message message)
        {
            var abstractionId = MiscService.GetNNarAbstractionId(Id, message.AppWrite.Register);
            SystemService.AddAbstraction(System, new NNAtomicRegister(abstractionId, System));

            var finalMessage = new Message
            {
                Type = Message.Types.Type.NnarWrite,
                NnarWrite = new NnarWrite
                {
                    Value = message.AppWrite.Value
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = abstractionId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleAppRead(Message message)
        {
            var abstractionId = MiscService.GetNNarAbstractionId(Id, message.AppRead.Register);

            SystemService.AddAbstraction(System, new NNAtomicRegister(abstractionId, System));

            var finalMessage = new Message
            {
                Type = Message.Types.Type.NnarRead,
                NnarRead = new NnarRead(),
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = abstractionId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleWriteReturn(Message message)
        {
            var registerName = MiscService.GetRegisterName(message.FromAbstractionId);

            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.AppWriteReturn,
                        AppWriteReturn = new AppWriteReturn
                        {
                            Register = registerName
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = System.HubProcessId
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleReadReturn(Message message)
        {
            var registerName = MiscService.GetRegisterName(message.FromAbstractionId);

            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.AppReadReturn,
                        AppReadReturn = new AppReadReturn
                        {
                            Register = registerName,
                            Value = message.NnarReadReturn.Value
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = System.HubProcessId
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleAppPropose(Message message)
        {
            var ucAbstractionId = MiscService.GetUniformConsensusAbstractionId(Id, message.AppPropose.Topic);
            SystemService.AddAbstraction(System, new UniformConsensus(ucAbstractionId, System));

            var finalMessage = new Message
            {
                Type = Message.Types.Type.UcPropose,
                UcPropose = new UcPropose
                {
                    Value = message.AppPropose.Value
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = ucAbstractionId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleUcDecide(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.AppDecide,
                        AppDecide = new AppDecide
                        {
                            Value = message.UcDecide.Value
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = System.HubProcessId
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);

            SystemService.StopTimers(System);
        }
    }
}
