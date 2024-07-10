using AMCDS.Protos;

namespace AMCDS.Models
{
    public abstract class Abstraction
    {
        public string Id { get; set; }
        public System System { get; set; }

        public Abstraction() { }

        public Abstraction(string id, System system)
        {
            Id = id;
            System = system;
        }

        public abstract bool Handle(Message message);
    }
}
