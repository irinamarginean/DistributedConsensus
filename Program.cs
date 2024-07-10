using AMCDS.Protos;
using AMCDS.Models;
using AMCDS.Services;
using System.Collections.Concurrent;
using Timer = System.Timers.Timer;

public class Program
{
    public static CancellationTokenSource cancellationTokenSource;

    private static void Main()
    {
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        var owner = "abc";
        var hubAddress = "127.0.0.1";
        var hubPort = 5000;
        int[] processPorts = { 5004, 5005, 5006 };
        var index = 1;
        var host = "127.0.0.1";

        var systems = new List<AMCDS.Models.System>();

        var hubProcessId = new ProcessId
        {
            Owner = "hub",
            Host = hubAddress,
            Port = hubPort
        };

        foreach (var port in processPorts)
        {
            var processId = new ProcessId
            {
                Owner = owner,
                Host = host,
                Port = port,
                Index = index
            };

            index++;

            var system = new AMCDS.Models.System
            {
                ProcessId = processId,
                HubProcessId = hubProcessId,
                Processes = new HashSet<ProcessId>(),
                MessageQueue = new BlockingCollection<Message>(),
                Abstractions = new ConcurrentDictionary<string, Abstraction>(),
                Timers = new List<Timer>()
            };

            Task.Run(() =>
            {
                SystemService.Run(system, cancellationTokenSource);
            });


            systems.Add(system);
        }

        Console.WriteLine("Press any key to stop the task...");
        Console.ReadKey();

        cancellationTokenSource.Cancel();

        Task.WaitAll();

        Console.WriteLine("Main thread ended.");
    }
}