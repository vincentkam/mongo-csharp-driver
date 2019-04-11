/* Copyright 2010-present MongoDB Inc.
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
using System.Linq;
using System.Net;
using System.Net.Sockets;
using MongoDB.Bson;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.TestHelpers;

namespace MongoDB.Driver.Tests
{
    /// <summary>
    /// A static class to handle online test configuration.
    /// </summary>
    public static class DriverTestConfiguration
    {
        // private static fields
        private static Lazy<MongoClient> __client;
        private static CollectionNamespace __collectionNamespace;
        private static DatabaseNamespace __databaseNamespace;
        private static Lazy<IEnumerable<IMongoClient>> __directClientsToShardRouters;

        // static constructor
        static DriverTestConfiguration()
        {
            __client = new Lazy<MongoClient>(() => new MongoClient(GetClientSettings()), true);
            __databaseNamespace = CoreTestConfiguration.DatabaseNamespace;
            __directClientsToShardRouters = new Lazy<IEnumerable<IMongoClient>>(
                () => CreateDirectClientsToHostsInConnectionString(CoreTestConfiguration.ConnectionStringWithMultipleShardRouters),
                isThreadSafe: true);
            __collectionNamespace = new CollectionNamespace(__databaseNamespace, "testcollection");
        }

        // public static properties
        /// <summary>
        /// Gets the test client.
        /// </summary>
        public static MongoClient Client
        {
            get { return __client.Value; }
        }

        /// <summary>
        /// Sequence of clients that connect directly to the shard routers
        /// </summary>
        public static IEnumerable<IMongoClient> DirectClientsToShardRouters
        {
            get => __directClientsToShardRouters.Value;
        }

        /// <summary>
        /// Gets the collection namespace.
        /// </summary>
        /// <value>
        /// The collection namespace.
        /// </value>
        public static CollectionNamespace CollectionNamespace
        {
            get { return __collectionNamespace; }
        }

        /// <summary>
        /// Gets the database namespace.
        /// </summary>
        /// <value>
        /// The database namespace.
        /// </value>
        public static DatabaseNamespace DatabaseNamespace
        {
            get { return __databaseNamespace; }
        }

        // public static methods
        public static IEnumerable<IMongoClient> CreateDirectClientsToServersInClientSettings(MongoClientSettings settings)
        {
            foreach (var server in settings.Servers)
            {
                var singleServerSettings = settings.Clone();
                singleServerSettings.Server = server;
                yield return new MongoClient(singleServerSettings);
            }
        }

        public static IEnumerable<IMongoClient> CreateDirectClientsToHostsInConnectionString(ConnectionString connectionString)
        {
            return CreateDirectClientsToServersInClientSettings(MongoClientSettings.FromConnectionString(connectionString.ToString()));
        }

        public static DisposableMongoClient CreateDisposableClient()
        {
            return CreateDisposableClient((MongoClientSettings s) => { });
        }

        public static DisposableMongoClient CreateDisposableClient(Action<ClusterBuilder> clusterConfigurator)
        {
            return CreateDisposableClient((MongoClientSettings s) => s.ClusterConfigurator = clusterConfigurator);
        }

        public static DisposableMongoClient CreateDisposableClient(
            Action<MongoClientSettings> clientSettingsConfigurator, 
            bool useMultipleShardRouters = false)
        {
            var connectionString = useMultipleShardRouters 
                ? CoreTestConfiguration.ConnectionStringWithMultipleShardRouters.ToString()
                : CoreTestConfiguration.ConnectionString.ToString();
            var clientSettings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
            clientSettingsConfigurator(clientSettings);
            var client = new MongoClient(clientSettings);
            return new DisposableMongoClient(client);
        }

        public static DisposableMongoClient CreateDisposableClient(EventCapturer capturer)
        {
            return CreateDisposableClient((ClusterBuilder c) => c.Subscribe(capturer));
        }

        public static MongoClientSettings GetClientSettings()
        {
            var connectionString = CoreTestConfiguration.ConnectionString.ToString();
            var clientSettings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));

            var serverSelectionTimeoutString = Environment.GetEnvironmentVariable("MONGO_SERVER_SELECTION_TIMEOUT_MS");
            if (serverSelectionTimeoutString == null)
            {
                serverSelectionTimeoutString = "30000";
            }
            clientSettings.ServerSelectionTimeout = TimeSpan.FromMilliseconds(int.Parse(serverSelectionTimeoutString));
            clientSettings.ClusterConfigurator = cb => CoreTestConfiguration.ConfigureLogging(cb);

            return clientSettings;
        }
    }
}
