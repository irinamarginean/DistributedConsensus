using AMCDS.Protos;
using AMCDS.Services;
using System.Collections.Concurrent;

namespace AMCDS.Models
{
    public class NNAtomicRegister : Abstraction
    {
        public static readonly string name = "nnar";
        private Tuple<int, int, Protos.Value> tuple = 
            new Tuple<int, int, Protos.Value>(0, 0, new Protos.Value { Defined = false });
        private int acks = 0;
        private Protos.Value writeVal = new Protos.Value { Defined = false };
        private int readId = 0;
        private ConcurrentDictionary<string, Tuple<int, int, Protos.Value>> readList = 
                new ConcurrentDictionary<string, Tuple<int, int, Protos.Value>>();
        private Protos.Value readVal = new Protos.Value { Defined = false };
        private bool isReading = false;

        public NNAtomicRegister() { }

        public NNAtomicRegister(string id, System system) : base(id, system) 
        {
            SystemService.AddAbstraction(system, new BestEffortBroadcast(MiscService.GetAbstractionChild(id, BestEffortBroadcast.name), system));
            SystemService.AddAbstraction(system, new PerfectLink(MiscService.GetAbstractionChild(id, PerfectLink.name), system));
        }
        
        public override bool Handle(Message message)
        {
            if (message.Type == Message.Types.Type.NnarRead)
            {
                HandleRead(message);
                return true;
            } 
            else if (message.Type == Message.Types.Type.NnarWrite)
            {
                HandleWrite(message);
                return true;
            }
            else if (message.Type == Message.Types.Type.BebDeliver)
            {
                if (message.BebDeliver.Message.Type == Message.Types.Type.NnarInternalRead)
                {
                    HandleInternalRead(message);
                    return true;
                }
                else if (message.BebDeliver.Message.Type == Message.Types.Type.NnarInternalWrite)
                {
                    HandleInternalWrite(message);
                    return true;
                }
            }
            else if (message.Type == Message.Types.Type.PlDeliver)
            {
                if (message.PlDeliver.Message.Type == Message.Types.Type.NnarInternalValue)
                {
                   if (message.PlDeliver.Message.NnarInternalValue.ReadId == readId)
                    {
                        HandleInternalValue(message);
                        return true;
                    }
                } 
                else if (message.PlDeliver.Message.Type == Message.Types.Type.NnarInternalAck)
                {
                    if (message.PlDeliver.Message.NnarInternalAck.ReadId == readId)
                    {
                        HandleInternalAck(message);
                        return true;
                    }
                }
            }

            return false;
        }

        private void HandleRead(Message message)
        {
            readId++;
            acks = 0;
            readList.Clear();
            isReading = true;

            var finalMessage = new Message
            {
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.NnarInternalRead,
                        NnarInternalRead = new NnarInternalRead
                        {
                            ReadId = readId
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

        private void HandleWrite(Message message)
        {
            readId++;
            writeVal = new Protos.Value
            {
                Defined = true,
                V = message.NnarWrite.Value.V
            };
            acks = 0;
            readList.Clear();

            var finalMessage = new Message
            {
                Type = Message.Types.Type.BebBroadcast,
                BebBroadcast = new BebBroadcast
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.NnarInternalRead,
                        NnarInternalRead = new NnarInternalRead
                        {
                            ReadId = readId
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

        private void HandleInternalRead(Message message)
        {
            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.NnarInternalValue,
                        NnarInternalValue = new NnarInternalValue
                        {
                            ReadId = message.BebDeliver.Message.NnarInternalRead.ReadId,
                            Timestamp = tuple.Item1,
                            WriterRank = tuple.Item2,
                            Value = tuple.Item3
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = message.BebDeliver.Sender
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                MessageUuid = Guid.NewGuid().ToString()
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleInternalValue(Message message)
        {
            readList[$"{message.PlDeliver.Sender.Owner}-{message.PlDeliver.Sender.Index}"] = 
                new Tuple<int, int, Protos.Value>(
                    message.PlDeliver.Message.NnarInternalValue.Timestamp,
                    message.PlDeliver.Message.NnarInternalValue.WriterRank,
                    message.PlDeliver.Message.NnarInternalValue.Value
                );

            if (readList.Count > System.Processes.Count / 2)
            {
                var highestTuple = GetHighestTuple(readList);
                readVal = highestTuple.Item3;
                readList.Clear();

                NnarInternalWrite internalWrite;
                if (isReading)
                {
                    internalWrite = new NnarInternalWrite
                    {
                       ReadId = message.PlDeliver.Message.NnarInternalValue.ReadId,
                       Timestamp = highestTuple.Item1,
                       WriterRank = highestTuple.Item2,
                       Value = highestTuple.Item3
                    };
                }
                else
                {
                    internalWrite = new NnarInternalWrite
                    {
                        ReadId = message.PlDeliver.Message.NnarInternalValue.ReadId,
                        Timestamp = highestTuple.Item1 + 1,
                        WriterRank = System.ProcessId.Rank,
                        Value = writeVal
                    };
                }

                var finalMessage = new Message
                {
                    Type = Message.Types.Type.BebBroadcast,
                    BebBroadcast = new BebBroadcast
                    {
                        Message = new Message
                        {
                            Type = Message.Types.Type.NnarInternalWrite,
                            NnarInternalWrite = internalWrite,
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
        }

        private void HandleInternalWrite(Message message)
        {
            var newTuple = new Tuple<int, int, Protos.Value>(
                message.BebDeliver.Message.NnarInternalWrite.Timestamp,
                message.BebDeliver.Message.NnarInternalWrite.WriterRank,
                message.BebDeliver.Message.NnarInternalWrite.Value
            );

            if (newTuple.IsLargerThan(tuple))
            {
                tuple = newTuple;
            }

            var finalMessage = new Message
            {
                Type = Message.Types.Type.PlSend,
                PlSend = new PlSend
                {
                    Message = new Message
                    {
                        Type = Message.Types.Type.NnarInternalAck,
                        NnarInternalAck = new NnarInternalAck
                        {
                            ReadId = message.BebDeliver.Message.NnarInternalWrite.ReadId
                        },
                        SystemId = System.SystemId,
                        FromAbstractionId = Id,
                        ToAbstractionId = Id,
                        MessageUuid = Guid.NewGuid().ToString()
                    },
                    Destination = message.BebDeliver.Sender
                },
                SystemId = System.SystemId,
                FromAbstractionId = Id,
                ToAbstractionId = MiscService.GetAbstractionChild(Id, PerfectLink.name),
                MessageUuid = Guid.NewGuid().ToString()
                
            };

            System.MessageQueue.Add(finalMessage);
        }

        private void HandleInternalAck(Message message)
        {
            acks++;

            if (acks > System.Processes.Count / 2)
            {
                var finalMessage = new Message
                {
                    SystemId = System.SystemId,
                    FromAbstractionId = Id,
                    ToAbstractionId = MiscService.GetAbstractionParent(Id),
                    MessageUuid = Guid.NewGuid().ToString()
                };

                acks = 0;

                if (isReading == true)
                {
                    isReading = false;

                    finalMessage.Type = Message.Types.Type.NnarReadReturn;
                    finalMessage.NnarReadReturn = new NnarReadReturn();
                    finalMessage.NnarReadReturn.Value = readVal;
                }
                else
                {
                    finalMessage.Type = Message.Types.Type.NnarWriteReturn;
                    finalMessage.NnarWriteReturn = new NnarWriteReturn();
                }

                System.MessageQueue.Add(finalMessage);
            }
        }

        private Tuple<int, int, Protos.Value> GetHighestTuple(ConcurrentDictionary<string, Tuple<int, int, Protos.Value>> readList)
        {
            if (readList.Count == 0)
            {
                return null;
            }

            var currentTuple = readList.Values.First();

            if (readList.Count == 1)
            {
                return currentTuple;
            }

            foreach (var val in readList.Values)
            {
                if (val.IsLargerThan(currentTuple))
                {
                    currentTuple = val;
                }
            }

            return currentTuple;
        }
    }
}
