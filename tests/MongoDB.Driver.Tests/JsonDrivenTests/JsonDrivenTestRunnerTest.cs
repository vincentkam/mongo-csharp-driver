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
using MongoDB.Bson;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public abstract class JsonDrivenTestRunnerTest : JsonDrivenCommandTest
    {
        private IClientSessionHandle _session;

        // protected constructors
        protected JsonDrivenTestRunnerTest(IJsonDrivenTestRunner testRunner, Dictionary<string, object> objectMap)
            : base(objectMap)
        {
            TestRunner = testRunner;
        }

        protected IJsonDrivenTestRunner TestRunner { get; }
        protected IServer PinnedServer => _session?.WrappedCoreSession?.CurrentTransaction?.PinnedServer;

        protected override void SetArgument(string name, BsonValue value)
        {
            switch (name)
            {
                case "session":
                    _session = (IClientSessionHandle)_objectMap[value.AsString];
                    return;
                default:
                    base.SetArgument(name, value);
                    return;
            }
        }
    }
}
