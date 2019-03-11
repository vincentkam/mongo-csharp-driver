using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests
{
    public interface ITestRunner : IDisposable
    {
        void ConfigureFailPoint(IServer server, ICoreSessionHandle session, BsonDocument failCommand);
        Task ConfigureFailpointAsync(IServer server, ICoreSessionHandle session, BsonDocument failCommand);
    }
}
