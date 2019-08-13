# .NET Driver Version 2.9.0 Release Notes

The main new features in 2.9.0 are:

* Sharded transactions using the same API as replica set transactions
* Sharded transactions pinning to a single mongos router
* Convenient API for transactions
* Support for message compression
* SRV polling for mongodb+srv connection scheme
* Retryable reads on by default
* Retryable writes on by default
* Update specification using an aggregation framework pipeline
* SCRAM-SHA authentication caching

An online version of these release notes is available at:

https://github.com/mongodb/mongo-csharp-driver/blob/master/Release%20Notes/Release%20Notes%20v2.9.0.md

The full list of JIRA issues that are currently scheduled to be resolved in this release is available at:

https://jira.mongodb.org/issues/?jql=project%20%3D%20CSHARP%20AND%20fixVersion%20%3D%202.9.0%20ORDER%20BY%20key%20ASC


Documentation on the .NET driver can be found at:

http://mongodb.github.io/mongo-csharp-driver/

Upgrading

There are no known backwards breaking changes in this release.
