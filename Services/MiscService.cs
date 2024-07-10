using AMCDS.Models;

namespace AMCDS.Services
{
    public static class MiscService
    {
        public static string GetAbstractionParent(string id)
        {
            if (string.IsNullOrEmpty(id)) 
            {
                return string.Empty;
            }

            var segments = id.Split('.');
            var parentId = segments.Length == 1 ? id : string.Join(".", segments[0..^1]);

            return parentId;
        }

        public static string GetAbstractionChild(string parentId, string childId)
        {
            if (string.IsNullOrEmpty(parentId))
            {
                return string.Empty;
            }

            if (string.IsNullOrEmpty(childId))
            {
                return parentId;
            }

            return $"{parentId}.{childId}";
        }

        public static string GetRegisterName(string abstractionId)
        {
            var sections = abstractionId.Split($"{NNAtomicRegister.name}[");
            if (sections.Length > 0) 
            {
                sections = sections[1].Split(']');

                return $"{sections[0]}";
            }

            return abstractionId;
        }

        public static string GetEpAbstractionId(string abstractionId)
        {
            var sections = abstractionId.Split($"{EpochConsensus.name}[");
            if (sections.Length > 0)
            {
                var epSplit = sections[1].Split(']');

                if (epSplit.Length > 0)
                {
                    return $"{sections[0]}{EpochConsensus.name}[{epSplit[0]}]";
                }
            }

            return abstractionId;
        }

        public static string GetNNarAbstractionId(string abstractionId, string registerName)
        {
            return $"{abstractionId}.{NNAtomicRegister.name}[{registerName}]";
        }

        public static string GetEpochConsensusAbstractionId(string abstractionId, int epochConsensusId)
        {
            return $"{abstractionId}.{EpochConsensus.name}[{epochConsensusId}]";
        }

        public static string GetUniformConsensusAbstractionId(string abstractionId, string topic)
        {
            return $"{abstractionId}.{UniformConsensus.name}[{topic}]";
        }

        public static bool IsLargerThan(this Tuple<int, int, Protos.Value> newTuple, Tuple<int, int, Protos.Value> oldTuple)
        {
            if (newTuple.Item1 < oldTuple.Item1)
            {
                return false;
            } 
            
            if (newTuple.Item1 == oldTuple.Item1)
            {
                if (newTuple.Item2 <= oldTuple.Item2)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
