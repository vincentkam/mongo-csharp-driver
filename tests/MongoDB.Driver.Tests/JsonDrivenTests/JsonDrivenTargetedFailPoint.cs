/* Copyright 2019–present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.TestHelpers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public sealed class JsonDrivenTargetedFailPointTest : JsonDrivenTestRunnerTest
    {
        private BsonDocument _failCommand;
        private readonly Action<IServer, ICoreSessionHandle, BsonDocument> _sendAndSaveFailPoint;
        private readonly Func<IServer, ICoreSessionHandle, BsonDocument, Task> _sendAndSaveFailPointAsync;

        public JsonDrivenTargetedFailPointTest(
            Action<IServer, ICoreSessionHandle, BsonDocument> sendAndSaveFailPoint,
            Func<IServer, ICoreSessionHandle, BsonDocument, Task> sendAndSaveFailPointAsync,
            Dictionary<string, object> objectMap)
            : base(objectMap)
        {
            _sendAndSaveFailPoint = sendAndSaveFailPoint;
            _sendAndSaveFailPointAsync = sendAndSaveFailPointAsync;
        }

        protected override void CallMethod(CancellationToken cancellationToken)
        {
            var pinnedServer = GetPinnedServer();
            Ensure.IsNotNull(pinnedServer, nameof(pinnedServer));
            _sendAndSaveFailPoint(GetPinnedServer(), NoCoreSession.NewHandle(), _failCommand);
        }

        protected override Task CallMethodAsync(CancellationToken cancellationToken)
        {
            var pinnedServer = GetPinnedServer();
            Ensure.IsNotNull(pinnedServer, nameof(pinnedServer));
            return _sendAndSaveFailPointAsync(GetPinnedServer(), NoCoreSession.NewHandle(), _failCommand);
        }


        public override void Assert()
        {
        }

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "session":
                    base.SetArgument(name, value);
                    return;
                case "failPoint":
                    _failCommand = (BsonDocument)value;
                    return;
            }

            base.SetArgument(name, value);
        }
    }
}
