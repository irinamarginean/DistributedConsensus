using AMCDS.Protos;
using System.Collections.Concurrent;

namespace AMCDS.Models
{
    public class Process
    {
        public ProcessId ProcessId { get; set; }
        public string ProcessName { get; set; }
        public ProcessId HubProcessId { get; set; }
        public HashSet<ProcessId> Processes { get; set; }
        //public ConcurrentDictionary<string, Abstraction> Abstractions { get; set; }
        public Task Task { get; set; }
        private BlockingCollection<Message> msgQueue { get; set; }

        public Process(ProcessId processId, ProcessId hubProcessId)
        {
            ProcessId = processId;
            HubProcessId = hubProcessId;

            Processes = new HashSet<ProcessId>();
            msgQueue = new BlockingCollection<Message>();

        }
    }
}
