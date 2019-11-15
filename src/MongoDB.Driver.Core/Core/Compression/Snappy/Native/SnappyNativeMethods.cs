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
using MongoDB.Driver.Core.Compression.Native;
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

        private static readonly IDictionary<SupportedPlatforms, string> __libraryPaths = new Dictionary<SupportedPlatforms, string>()
        {
            { SupportedPlatforms.Windows, string.Empty }, // bin folder
            // On Linux, the snappy library depends on the Snappy package.
            // For Debian/Ubuntu: apt-get install libsnappy1 libsnappy-dev
            { SupportedPlatforms.Linux, "../../native/linux/" }, // todo: check
            { SupportedPlatforms.MacOs, "../../native/osx/" } // todo: check
        };

        private readonly ISnappyNativeMethods _snappyNativeMethods;

        private SnappyNativeMethods()
        {
            var libraryLoaderSource = new LibraryLoaderSource(__libraryPaths, is64Bit => is64Bit ? "snappy64.dll" : "snappy32.dll");
            if (libraryLoaderSource.Is64BitnessPlatform)
            {
                _snappyNativeMethods = new SnappyNativeMethods64(libraryLoaderSource);
            }
            else
            {
                _snappyNativeMethods = new SnappyNativeMethods32(libraryLoaderSource);
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

            public SnappyNativeMethods32(ILibraryLoaderSource libraryLoaderSource)
            {
                _snappy_compress = libraryLoaderSource.GetFunction<Delegates32.snappy_compress>(nameof(snappy_compress));
                _snappy_max_compressed_length = libraryLoaderSource.GetFunction<Delegates32.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
                _snappy_uncompress = libraryLoaderSource.GetFunction<Delegates32.snappy_uncompress>(nameof(snappy_uncompress));
                _snappy_uncompressed_length = libraryLoaderSource.GetFunction<Delegates32.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
                _snappy_validate_compressed_buffer = libraryLoaderSource.GetFunction<Delegates32.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
            }

            // public methods
            public SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                return _snappy_compress(input, input_length, output, ref output_length);
            }

            public int snappy_max_compressed_length(int input_length)
            {
                return _snappy_max_compressed_length(input_length);
            }

            public SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                return _snappy_uncompress(input, input_length, output, ref output_length);
            }

            public SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
            {
                return _snappy_uncompressed_length(input, input_length, out output_length);
            }

            public SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
            {
                return _snappy_validate_compressed_buffer(input, input_length);
            }

            // nested types
            private class Delegates32
            {
                public delegate SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length);
                public delegate int snappy_max_compressed_length(int input_length);
                public delegate SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length);
                public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length);
                public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length);
            }
        }

        private class SnappyNativeMethods64 : ISnappyNativeMethods
        {
            private readonly Delegates64.snappy_compress _snappy_compress;
            private readonly Delegates64.snappy_max_compressed_length _snappy_max_compressed_length;
            private readonly Delegates64.snappy_uncompress _snappy_uncompress;
            private readonly Delegates64.snappy_uncompressed_length _snappy_uncompressed_length;
            private readonly Delegates64.snappy_validate_compressed_buffer _snappy_validate_compressed_buffer;

            public SnappyNativeMethods64(ILibraryLoaderSource libraryLoaderSource)
            {
                _snappy_compress = libraryLoaderSource.GetFunction<Delegates64.snappy_compress>(nameof(snappy_compress));
                _snappy_max_compressed_length = libraryLoaderSource.GetFunction<Delegates64.snappy_max_compressed_length>(nameof(snappy_max_compressed_length));
                _snappy_uncompress = libraryLoaderSource.GetFunction<Delegates64.snappy_uncompress>(nameof(snappy_uncompress));
                _snappy_uncompressed_length = libraryLoaderSource.GetFunction<Delegates64.snappy_uncompressed_length>(nameof(snappy_uncompressed_length));
                _snappy_validate_compressed_buffer = libraryLoaderSource.GetFunction<Delegates64.snappy_validate_compressed_buffer>(nameof(snappy_validate_compressed_buffer));
            }

            // public methods
            public SnappyStatus snappy_compress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var longOutput_length = (long)output_length;
                var status = _snappy_compress(input, input_length, output, ref longOutput_length);
                output_length = (int)longOutput_length;
                return status;
            }

            public int snappy_max_compressed_length(int input_length)
            {
                return (int)_snappy_max_compressed_length(input_length);
            }

            public SnappyStatus snappy_uncompress(IntPtr input, int input_length, IntPtr output, ref int output_length)
            {
                var longOutput_length = (long)output_length;
                var status = _snappy_uncompress(input, input_length, output, ref longOutput_length);
                output_length = (int)longOutput_length;
                return status;
            }

            public SnappyStatus snappy_uncompressed_length(IntPtr input, int input_length, out int output_length)
            {
                var status = _snappy_uncompressed_length(input, input_length, out var longOutput_length);
                output_length = (int)longOutput_length;
                return status;
            }

            public SnappyStatus snappy_validate_compressed_buffer(IntPtr input, int input_length)
            {
                return _snappy_validate_compressed_buffer(input, input_length);
            }

            // nested types
            private class Delegates64
            {
                public delegate SnappyStatus snappy_compress(IntPtr input, long input_length, IntPtr output, ref long output_length);
                public delegate long snappy_max_compressed_length(long input_length);
                public delegate SnappyStatus snappy_uncompress(IntPtr input, long input_length, IntPtr output, ref long output_length);
                public delegate SnappyStatus snappy_uncompressed_length(IntPtr input, long input_length, out long output_length);
                public delegate SnappyStatus snappy_validate_compressed_buffer(IntPtr input, long input_length);
            }
        }
    }
}
