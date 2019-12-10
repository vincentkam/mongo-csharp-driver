/* Copyright 2019-present MongoDB Inc.
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
using System.IO;
using System.Reflection;

namespace MongoDB.Driver.Core.NativeLibraryLoader
{
    internal abstract class RelativeLibraryLocatorBase : ILibraryLocator
    {
        // public methods
        public string GetLibraryAbsolutePath(SupportedPlatform currentPlatform)
        {
            var relativePath = GetLibraryRelativePath(currentPlatform);
            return GetAbsolutePath(relativePath);
        }

        public virtual Assembly GetLibraryBaseAssembly()
        {
            return typeof(RelativeLibraryLocatorBase).GetTypeInfo().Assembly;
        }

        public virtual string GetLibraryBasePath()
        {
            // In the nuget package, get the shared library from a relative path of this assembly
            // Also, when running locally, get the shared library from a relative path of this assembly
            var assembly = typeof(LibraryLoader).GetTypeInfo().Assembly;
            var location = assembly.Location;
            string basePath = Path.GetDirectoryName(location);
            return Path.GetDirectoryName(basePath);
        }

        public abstract string GetLibraryRelativePath(SupportedPlatform currentPlatform);

        // private methods
        private string FindLibraryOrThrow(string[] basePaths, string[] suffixPaths, string library)
        {
            var failedPaths = new List<string>();
            foreach (var basePath in basePaths)
            {
                foreach (var suffix in suffixPaths)
                {
                    string path = Path.Combine(basePath, suffix, library);
                    if (File.Exists(path))
                    {
                       return path;
                    }
                    failedPaths.Add(path);
                }
            }
            throw new FileNotFoundException("Could not find: " + library + " --\n Tried: " + string.Join(",", failedPaths));
       }

        private string GetAbsolutePath(string relativePath)
        {
            var basePath = GetLibraryBasePath();
            return FindLibraryOrThrow(new [] {basePath, ""}, new[] {relativePath, ""}, "snappy64.dll");
        }
    }
}
