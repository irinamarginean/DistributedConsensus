using AMCDS.Protos;
using AMCDS.Services;
using System.Collections.Concurrent;

namespace AMCDS.Models
{
    public class EpochConsensus : Abstraction
    {
        public static readonly string name = "ep";

        private Tuple<int, Value> state;
        private int epochTimestamp;
        private ProcessId leader;
        private Value temporaryValue;
        private int acceptedCount;
        private ConcurrentDictionary<ProcessId, Tuple<int, Value>> states;
        private bool isHalted;

        public EpochConsensus() { }

        public EpochConsensus(string id, System system, Tuple<int, Value> state, int ets, ProcessId leader) : base(id, system)
        {
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));
            SystemService.AddAbstraction(system, new BestEffortBroadcast(MiscService.GetAbstractionChild(id, BestEffortBroadcast.name), system));

            this.state = new Tuple<int, Value>(state.Item1, state.Item2);
            epochTimestamp = ets;
            this.leader = leader;
            temporaryValue = new Value { Defined = false };
            acceptedCount = 0;
            states = new ConcurrentDictionary<ProcessId, Tuple<int, Value>>();
            isHalted = false;
        }

        public override bool Handle(Message message)
        {
            Console.WriteLine(System.ProcessId.Owner + " " + System.ProcessId.Index + " => " + message.Type + " from: " + message.FromAbstractionId + " ; to: " + message.ToAbstractionId);

            if (isHalted)
            {
                return true;
            }

            if (message.Type == Message.Types.Type.EpPropose)
            {
                if (leader.Rank != System.ProcessId.Rank) { return false; }

                HandleEpPropose(message);

                return true;
            }
            else if (message.Type == Message.Types.Type.BebDeliver)
            {
                if (message.BebDeliver.Message.Type == Message.Types.Type.EpInternalRead)
                {
                    HandleEpInternalRead(message);

                    return true;
                }
                else if (message.BebDeliver.Message.Type == Message.Types.Type.EpInternalWrite)
                {
                    HandleEpInternalWrite(message);

                    return true;
                }
                else if (message.BebDeliver.Message.Type == Message.Types.Type.EpInternalDecided)
                {
                    HandleEpInternalDecided(message);

                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.PlDeliver)
            {
                if (message.PlDeliver.Message.Type == Message.Types.Type.EpInternalState)
                {
                    if (leader.Rank != System.ProcessId.Rank) { return false; }

                    HandleEpInternalState(message);

                    return true;
                }
                else if (message.PlDeliver.Message.Type == Message.Types.Type.EpInternalAccept)
                {
                    if (leader.Rank != System.ProcessId.Rank) { return false; }

                    HandleEpInternalAccept(message);

                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.EpAbort)
            {
                HandleEpAbort(message);

                return true;
            }

            return false;
        }

        private void HandleEpPropose(Message message)
        {
            temporaryValue = message.EpPropose.Value;

            Console.WriteLine($"started epoch {epochTimestamp}");

            var finalMessage = new Message
            {
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.EpInternalRead,
                        EpInternalRead = new EpInternalRead(),
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

        private void HandleEpInternalRead(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.EpInternalState,
                        EpInternalState = new EpInternalState
                        {
                            ValueTimestamp = state.Item1,
                            Value = state.Item2
                        },
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

        private void HandleEpInternalWrite(Message message)
        {
            state = new Tuple<int, Value>(epochTimestamp, message.BebDeliver.Message.EpInternalWrite.Value);

            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.EpInternalAccept,
                        EpInternalAccept = new EpInternalAccept(),
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

        private void HandleEpInternalDecided(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.EpDecide,
                EpDecide = new EpDecide
                {
                    Ets = epochTimestamp,
                    Value = message.BebDeliver.Message.EpInternalDecided.Value
                },
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionParent(Id),
                SystemId = System.SystemId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleEpInternalState(Message message)
        {
            states[message.PlDeliver.Sender] =
                new Tuple<int, Value>(
                    message.PlDeliver.Message.EpInternalState.ValueTimestamp,
                    message.PlDeliver.Message.EpInternalState.Value);

            HandleStatesChange();
        }

        private void HandleStatesChange()
        {
            if (states.Count > (System.Processes.Count / 2))
            {
                var highestState = states.Values.OrderByDescending(x => x.Item1).FirstOrDefault();

                if (highestState != null && highestState.Item2.Defined == true)
                {
                    temporaryValue = highestState.Item2;
                }

                states.Clear();

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EpInternalWrite,
                            EpInternalWrite = new EpInternalWrite
                            {
                                Value = temporaryValue
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

        private void HandleEpInternalAccept(Message message)
        {
            acceptedCount++;

            HandleAcceptedChange();
        }

        private void HandleAcceptedChange()
        {
            if (acceptedCount > System.Processes.Count / 2)
            {
                acceptedCount = 0;

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.EpInternalDecided,
                            EpInternalDecided = new EpInternalDecided
                            {
                                Value = temporaryValue
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

        private void HandleEpAbort(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.EpAborted,
                EpAborted = new EpAborted
                {
                    Ets = epochTimestamp,
                    Value = state.Item2,
                    ValueTimestamp = state.Item1
                },
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionParent(Id),
                SystemId = System.SystemId,
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);

            isHalted = true;
        }
    }
}
