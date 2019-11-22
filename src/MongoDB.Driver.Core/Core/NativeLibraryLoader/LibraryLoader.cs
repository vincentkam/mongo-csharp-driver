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

using MongoDB.Driver.Core.Misc;
using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace MongoDB.Driver.Core.NativeLibraryLoader
{
    internal enum SupportedPlatform
    {
        Windows,
        Linux,
        MacOS
    }

    internal class LibraryLoader
    {
        private readonly INativeLibraryLoader _nativeLoader;

        public LibraryLoader(Func<SupportedPlatform, string> libraryLocator)
        {
            Ensure.IsNotNull(libraryLocator, nameof(libraryLocator));
            ThrowIfNotSupportedPlatform();
            _nativeLoader = CreateNativeLoader(libraryLocator);
        }

        // public methods
        public T GetDelegate<T>(string name)
        {
            IntPtr ptr = _nativeLoader.GetFunctionPointer(name);
            if (ptr == IntPtr.Zero)
            {
                throw new TypeLoadException($"The function {name} was not found.");
            }

            return Marshal.GetDelegateForFunctionPointer<T>(ptr);
        }

        // private methods
        private INativeLibraryLoader CreateNativeLoader(Func<SupportedPlatform, string> libraryLocator)
        {
            var currentPlatform = GetCurrentPlatform();
            var relativePath = libraryLocator(currentPlatform);
            var absolutePath = GetAbsolutePath(relativePath);
            return CreateNativeLoader(currentPlatform, absolutePath);
        }

        private INativeLibraryLoader CreateNativeLoader(SupportedPlatform currentPlatform, string libraryPath)
        {
            switch (currentPlatform)
            {
                case SupportedPlatform.Linux:
                    return new LinuxLibraryLoader(libraryPath);
                case SupportedPlatform.MacOS:
                    return new DarwinLibraryLoader(libraryPath);
                case SupportedPlatform.Windows:
                    return new WindowsLibraryLoader(libraryPath);
                default:
                    throw new PlatformNotSupportedException($"Unexpected platform {currentPlatform}.");
            }
        }

        private string FindLibraryOrThrow(string basePath, string relativePath)
        {
            var fullPath = Path.Combine(basePath, relativePath);
            if (File.Exists(fullPath))
            {
                return fullPath;
            }

            throw new FileNotFoundException($"Could not find library {fullPath}.");
        }

        private string GetAbsolutePath(string relativePath)
        {
            var assembly = typeof(LibraryLoader).GetTypeInfo().Assembly;
            var codeBase = assembly.CodeBase;
            var uri = new Uri(codeBase);
            var location = uri.AbsolutePath;
            var basepath = Path.GetDirectoryName(location);

            return FindLibraryOrThrow(basepath, relativePath);
        }

        private SupportedPlatform GetCurrentPlatform()
        {
#if NETSTANDARD1_5
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return SupportedPlatform.MacOS;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return SupportedPlatform.Linux;
            }
            else
#endif
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return SupportedPlatform.Windows;
            }

            throw new InvalidOperationException("Current platform is not supported by LibraryLoader.");
        }

        private void ThrowIfNotSupportedPlatform()
        {
#if NET452 || NETSTANDARD2_0
            var is64Bit = Environment.Is64BitProcess;
#else
            var is64Bit = IntPtr.Size == 8;
#endif
            if (!is64Bit)
            {
                throw new PlatformNotSupportedException("Native libraries can be loaded only in a 64-bit process.");
            }
        }
    }
}
