#!/usr/bin/python3
# coding=utf-8

"""
ChopZip
(c) 2009-2013, 2017, Iñaki Silanes

LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License (version 2 or later),
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details (http://www.gnu.org/licenses/gpl.txt).

DESCRIPTION

(De)compresses files with XZ/gzip in parallel.

USAGE

% chopzip [options] file(s)

for options:

% chopzip -h 
"""

# Standard libs:
import os
import gzip
import lzma
import argparse
import multiprocessing as mp

# Constants:
MB = 1024*1024

# Functions:
def main():
    """Main function."""

    # Parse command line arguments:
    opts = parse_args()

    # Choose method:
    if opts.gzip:
        Method = Gzip
    else:
        Method = XZ

    # Compress each file requested:
    for input_fn in opts.positional:
        method = Method(opts, input_fn)
        method.run()

def parse_args():
    """Read and parse arguments"""
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("positional",
                        nargs='+',
                        metavar="X",
                        help="Positional arguments")
    
    parser.add_argument("-n", "--ncores",
                        help="Amount of cores. Default: detect.",
                        type=int,
                        default=0)
    
    parser.add_argument("-k", "--keep-input",
                        help="Keep uncompressed file. Default: delete it, once compressed.",
                        action="store_true",
                        default=False)
    
    parser.add_argument("-s", "--chunk-size",
                        help="Read chunk size, in MB. Default: automatic.",
                        type=float,
                        default=None)
    
    parser.add_argument("--gzip",
                        help="Compress using gzip. Default: use XZ.",
                        action="store_true",
                        default=False)
    
    
    return parser.parse_args()


# Classes:
class ChopZip(object):
    """Class with all methods."""

    MINIMUM_CHUNK_SIZE = 1*MB
    MAXIMUM_CHUNK_SIZE = 25*MB
    DEFAULT_NPROCS = 1

    def __init__(self, opts, input_fn):
        self.opts = opts
        self.input_fn = input_fn
        self.pool = mp.Pool(processes=self.ncores)

    def run(self):
        """Run the whole thing."""

        # Compression loop:
        with open(self.input_fn, 'rb') as self.fhandle_in:
            with open(self.output_fn, "wb") as self.fhandle_out:
                for compressed_chunk in self.pool.imap(lzma.compress, self.chunk_reader()):
                    self.write_chunk(compressed_chunk)

        # Clean:
        self.clean()

    def clean(self):
        """Perform required cleanup."""

        # Close process pool:
        self.pool.close()
        self.pool.join()

        # Delete input file, if not requested not to:
        if not self.opts.keep_input:
            os.unlink(self.input_fn)

    def chunk_reader(self):
        """Generator for reading input as chunks."""

        while True:
            chunk = self.read_chunk()
            if not chunk:
                break

            yield chunk

    def read_chunk(self):
        """Read a single data chunk, and return it."""

        return self.fhandle_in.read(self.chunk_size)

    def write_chunk(self, chunk):
        """Write chunk to disk."""
        
        self.fhandle_out.write(chunk)

    @property
    def output_fn(self):
        """Return name of output file."""

        return ".".join([self.input_fn, self.EXTENSION])

    @property
    def ncores(self):
        """Return amount of cores to use/processes to spawn."""

        if self.opts.ncores:
            return self.opts.ncores
        else:
            try:
                return mp.cpu_count()
            except:
                return self.DEFAULT_NPROCS

    @property
    def chunk_size(self):
        """Return size of data chunk to be read and compressed per process."""

        # If requested by user, use that:
        if self.opts.chunk_size:
            return self.opts.chunk_size * MB

        # First try, file size divided by amount of cores:
        cs = os.path.getsize(self.input_fn) // self.ncores + 1

        if cs < self.MINIMUM_CHUNK_SIZE:
            return self.MINIMUM_CHUNK_SIZE

        if cs > self.MAXIMUM_CHUNK_SIZE:
            return self.MAXIMUM_CHUNK_SIZE

        return cs

class XZ(ChopZip):
    """Class for using XZ."""

    EXTENSION = "xz"

    def run(self):
        """Run the whole thing."""

        # Compression loop:
        with open(self.input_fn, 'rb') as self.fhandle_in:
            with open(self.output_fn, "wb") as self.fhandle_out:
                for compressed_chunk in self.pool.imap(lzma.compress, self.chunk_reader()):
                    self.write_chunk(compressed_chunk)

        # Clean:
        self.clean()

class Gzip(ChopZip):
    """Class for using gzip."""

    EXTENSION = "gz"

    def run(self):
        """Run the whole thing."""

        # Compression loop:
        with open(self.input_fn, 'rb') as self.fhandle_in:
            with open(self.output_fn, "wb") as self.fhandle_out:
                for compressed_chunk in self.pool.imap(gzip.compress, self.chunk_reader()):
                    self.write_chunk(compressed_chunk)

        # Clean:
        self.clean()


# Code:
if __name__ == "__main__":
    main()
