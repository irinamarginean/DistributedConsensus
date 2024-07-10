using AMCDS.Protos;
using System.Net;
using System.Net.Sockets;

namespace AMCDS.Services
{
    public static class TcpService
    {
        public static void Listen(CancellationTokenSource cancellationTokenSource,
                IPAddress address, int port, Action<Message> action)
        {
            var listener = new TcpListener(address, port);
            listener.Start();

            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                listener.BeginAcceptTcpClient(new AsyncCallback(ar => ProcessConnection(ar, action)), listener);
            }

            listener.Stop();
        }

        private static void ProcessConnection(IAsyncResult ar, Action<Message> action)
        {
            var listener = ar.AsyncState as TcpListener;

            if (listener == null)
                return;

            using var connection = listener.EndAcceptTcpClient(ar);
            using var networkStream = connection.GetStream();
            using var reader = new BinaryReader(networkStream);
            byte[] buffer = new byte[1024];

            buffer = reader.ReadBytes(4);
            Array.Reverse(buffer, 0, 4);
            int messageLength = BitConverter.ToInt32(buffer[0..4]);

            buffer = reader.ReadBytes(messageLength);

            var message = Message.Parser.ParseFrom(buffer[0..messageLength]);

            action(message);
        }

        public static async void Send(byte[] data, string address, int port)
        {
            var client = new TcpClient(address, port);
            var stream = client.GetStream();
            var binaryWriter = new BinaryWriter(stream);

            var bigEndianData = BitConverter.GetBytes(data.Length);
            Array.Reverse(bigEndianData);

            binaryWriter.Write(bigEndianData);
            binaryWriter.Write(data);

            binaryWriter.Close();
            stream.Close();
            client.Close();
        }
    }
}
