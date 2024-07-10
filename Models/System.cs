using AMCDS.Protos;
using System.Collections.Concurrent;
using Timer = System.Timers.Timer;

namespace AMCDS.Models
{
    public class System
    {
        public string SystemId { get; set; }
        public ProcessId ProcessId { get; set; }
        public string ProcessName { get; set; }
        public ProcessId HubProcessId { get; set; }
        public HashSet<ProcessId> Processes { get; set; }
        public ConcurrentDictionary<string, Abstraction> Abstractions { get; set; }
        public BlockingCollection<Message> MessageQueue { get; set; }
        public List<Timer> Timers { get; set; }
        public bool AreTimersActive { get; set; }
        public bool IsConsensusInProgress { get; set; }
    }
}
