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
using MongoDB.Driver.Core.Bindings;

namespace MongoDB.Driver.Core
{
    internal static class TransactionHelper
    {
        internal static void UnpinServerIfNeeded(ICoreSession session, Exception exception)
        {
            if (session.IsInTransaction && ShouldExceptionUnpinServer(exception))
            {
                session.CurrentTransaction.PinnedServer = null;
            }
        }

        internal static void UnpinServerIfNeededOnRetryableCommit(CoreTransaction transaction, Exception exception)
        {
            if (ShouldRetryableCommitExceptionUnpinServer(exception))
            {
                transaction.PinnedServer = null;
            }
        }

        private static bool ShouldExceptionUnpinServer(Exception exception)
        {
            return
                exception is MongoException mongoException &&
                (mongoException.HasErrorLabel("TransientTransactionError") ||
                 mongoException.HasErrorLabel("UnknownTransactionCommitResult"));
        }

        private static bool ShouldRetryableCommitExceptionUnpinServer(Exception exception)
        {
            return
                exception is MongoException mongoException &&
                mongoException.HasErrorLabel("UnknownTransactionCommitResult");
        }
    }
}