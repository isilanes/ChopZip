# Standard libs:
import os
import lzma
import time
import pickle
import argparse
import multiprocessing as mp

# Functions:
def main():
    """Main function."""

    # Parse command line arguments:
    opts = parse_args()

    # Compress each file requested:
    for input_fn in opts.positional:
        CZ = ChopZip(opts, input_fn)
        CZ.run()

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
    
    
    return parser.parse_args()


# Classes:
class ChopZip(object):
    """Class with all methods."""

    MINIMUM_CHUNK_SIZE = 1*1024*1024
    MAXIMUM_CHUNK_SIZE = 10*1024*1024
    DEFAULT_NPROCS = 1
    EXTENSION = "xz"

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
            else:
                return self.DEFAULT_NPROCS

    @property
    def chunk_size(self):
        """Return size of data chunk to be read and compressed per process."""

        # First try, file size divided by amount of cores:
        cs = os.path.getsize(self.input_fn) // self.ncores + 1

        if cs < self.MINIMUM_CHUNK_SIZE:
            return self.MINIMUM_CHUNK_SIZE

        if cs > self.MAXIMUM_CHUNK_SIZE:
            return self.MAXIMUM_CHUNK_SIZE

        return cs


# Code:
if __name__ == "__main__":
    main()
