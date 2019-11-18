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
using System.Runtime.InteropServices;
using MongoDB.Driver.Core.Compression.Native;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.NativeLibraryLoader;

namespace MongoDB.Driver.Core.Compression.Snappy.Native
{
    internal interface ISnappyNativeMethods
    {
        SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length);

        int snappy_max_compressed_length(int input_length);

        SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length);

        SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length);

        SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length);
    }

    internal class SnappyNativeMethods : ISnappyNativeMethods
    {
        #region static
        private static ISnappyNativeMethods __instance;
        public static ISnappyNativeMethods Instance => __instance ?? (__instance = new SnappyNativeMethods());
        #endregion

        private readonly IDictionary<SupportedPlatforms, string> _libraryPaths = new Dictionary<SupportedPlatforms, string>
        {
            { SupportedPlatforms.Windows, string.Empty }, // bin folder
            // On Linux, the snappy library depends on the Snappy package.
            // For Debian/Ubuntu: apt-get install libsnappy1 libsnappy-dev
            { SupportedPlatforms.Linux, "../../native/linux/" }, // todo: check
            { SupportedPlatforms.MacOS, "../../native/osx/" } // todo: check
        };

        private readonly ISnappyNativeMethods _snappyNativeMethods;

        // private constructor
        private SnappyNativeMethods()
        {
            var librarySource = new LibrarySource(_libraryPaths, is64Bit => is64Bit ? "snappy64.dll" : "snappy32.dll");
            if (librarySource.Is64BitnessPlatform)
            {
                _snappyNativeMethods = new SnappyNativeMethods64(librarySource);
            }
            else
            {
                _snappyNativeMethods = new SnappyNativeMethods32(librarySource);
            }
        }

        // public methods
        public SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
        {
            return _snappyNativeMethods.snappy_compress(input, input_length, output, ref output_length);
        }

        public int snappy_max_compressed_length(int input_length)
        {
            return _snappyNativeMethods.snappy_max_compressed_length(input_length);
        }

        public SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
        {
            return _snappyNativeMethods.snappy_uncompress(input, input_length, output, ref output_length);
        }

        public SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
        {
            return _snappyNativeMethods.snappy_uncompressed_length(input, input_length, out output_length);
        }

        public SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
        {
            return _snappyNativeMethods.snappy_validate_compressed_buffer(input, input_length);
        }

        // nested types
        private class SnappyNativeMethods32 : ISnappyNativeMethods
        {
            private readonly Delegates32.snappy_compress _snappy_compress;
            private readonly Delegates32.snappy_max_compressed_length _snappy_max_compressed_length;
            private readonly Delegates32.snappy_uncompress _snappy_uncompress;
            private readonly Delegates32.snappy_uncompressed_length _snappy_uncompressed_length;
            private readonly Delegates32.snappy_validate_compressed_buffer _snappy_validate_compressed_buffer;

            public SnappyNativeMethods32(ILibrarySource librarySource)
            {
                Ensure.IsNotNull(librarySource, nameof(librarySource));

                _snappy_compress = librarySource.GetFunction<Delegates32.snappy_compress>(nameof(snappy_compress));
                _snappy_max_compressed_length = librarySource.GetFunction<Delegates32.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
                _snappy_uncompress = librarySource.GetFunction<Delegates32.snappy_uncompress>(nameof(snappy_uncompress));
                _snappy_uncompressed_length = librarySource.GetFunction<Delegates32.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
                _snappy_validate_compressed_buffer = librarySource.GetFunction<Delegates32.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
            }

            // public methods
            public SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var uintOutput_length = (uint) output_length;
                var result = _snappy_compress(input, (uint)input_length, output, ref uintOutput_length);
                output_length = (int) uintOutput_length;
                return result;
            }

            public int snappy_max_compressed_length(int input_length)
            {
                return (int)_snappy_max_compressed_length((uint)input_length);
            }

            public SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var uintOutput_length = (uint)output_length;
                var result = _snappy_uncompress(input, (uint)input_length, output, ref uintOutput_length);
                output_length = (int) uintOutput_length;
                return result;
            }

            public SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
            {
                var result = _snappy_uncompressed_length(input, (uint)input_length, out var uintOutput_length);
                output_length = (int) uintOutput_length;
                return result;
            }

            public SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
            {
                return _snappy_validate_compressed_buffer(input, (uint)input_length);
            }

            // nested types
            private class Delegates32
            {
                [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
                public delegate SnappyStatus snappy_compress(IntPtr input, uint input_length, IntPtr output, ref uint output_length);
                [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
                public delegate uint snappy_max_compressed_length(uint input_length);
                [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
                public delegate SnappyStatus snappy_uncompress(IntPtr input, uint input_length, IntPtr output, ref uint output_length);
                [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
                public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, uint input_length, out uint output_length);
                [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
                public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, uint input_length);
            }
        }

        private class SnappyNativeMethods64 : ISnappyNativeMethods
        {
            private readonly Delegates64.snappy_compress _snappy_compress;
            private readonly Delegates64.snappy_max_compressed_length _snappy_max_compressed_length;
            private readonly Delegates64.snappy_uncompress _snappy_uncompress;
            private readonly Delegates64.snappy_uncompressed_length _snappy_uncompressed_length;
            private readonly Delegates64.snappy_validate_compressed_buffer _snappy_validate_compressed_buffer;

            public SnappyNativeMethods64(ILibrarySource librarySource)
            {
                Ensure.IsNotNull(librarySource, nameof(librarySource));

                _snappy_compress = librarySource.GetFunction<Delegates64.snappy_compress>(nameof(snappy_compress));
                _snappy_max_compressed_length = librarySource.GetFunction<Delegates64.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
                _snappy_uncompress = librarySource.GetFunction<Delegates64.snappy_uncompress>(nameof(snappy_uncompress));
                _snappy_uncompressed_length = librarySource.GetFunction<Delegates64.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
                _snappy_validate_compressed_buffer = librarySource.GetFunction<Delegates64.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
            }

            // public methods
            public SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var ulongOutput_length = (ulong)output_length;
                var status = _snappy_compress(input, (ulong)input_length, output, ref ulongOutput_length);
                output_length = (int)ulongOutput_length;
                return status;
            }

            public int snappy_max_compressed_length(int input_length)
            {
                return (int)_snappy_max_compressed_length((ulong)input_length);
            }

            public SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var ulongOutput_length = (ulong)output_length;
                var status = _snappy_uncompress(input, (ulong)input_length, output, ref ulongOutput_length);
                output_length = (int)ulongOutput_length;
                return status;
            }

            public SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
            {
                var status = _snappy_uncompressed_length(input, (ulong)input_length, out var ulongOutput_length);
                output_length = (int)ulongOutput_length;
                return status;
            }

            public SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
            {
                return _snappy_validate_compressed_buffer(input, (ulong)input_length);
            }

            // nested types
            private class Delegates64
            {
                public delegate SnappyStatus snappy_compress(IntPtr input, ulong input_length, IntPtr output, ref ulong output_length);
                public delegate ulong snappy_max_compressed_length(ulong input_length);
                public delegate SnappyStatus snappy_uncompress(IntPtr input, ulong input_length, IntPtr output, ref ulong output_length);
                public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, ulong input_length, out ulong output_length);
                public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, ulong input_length);
            }
        }
    }
}
