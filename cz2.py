# Standard libs:
import lzma
import time
import pickle
import multiprocessing as mp

# Functions:
def main():
    """Main function."""

    fn_in = "somefile.big"
    fn_out = "out.xz"

    CZ = ChopZip(fn_in, fn_out)
    CZ.run()


# Classes:
class ChopZip(object):
    """Class with all methods."""

    CHUNK_SIZE = 20*1024*1024
    NPROCS = 4

    def __init__(self, fn_in, fn_out):
        self.fn_in = fn_in
        self.fn_out = fn_out
        self.pool = mp.Pool(processes=self.NPROCS)

    def run(self):
        """Run the whole thing."""

        # Compression loop:
        with open(self.fn_in, 'rb') as self.fin:
            with open(self.fn_out, "wb") as self.fout:
                for compressed_chunk in self.pool.imap(lzma.compress, self.chunk_reader()):
                    self.write_chunk(compressed_chunk)

        # Clean:
        self.clean()

    def clean(self):
        """Perform required cleanup."""

        # Close process pool:
        self.pool.close()
        self.pool.join()

        # Save any remaining compressed chunk:
        for i in sorted(self.compressed_chunks):
            self.fout.write(self.compressed_chunks[i])

    def chunk_reader(self):
        """Generator for reading input as chunks."""

        while True:
            chunk = self.read_chunk()
            if not chunk:
                break

            yield chunk

    def read_chunk(self):
        """Read a single data chunk, and return it."""

        return self.fin.read(self.CHUNK_SIZE)

    def save_ordered_chunks_to_disk(self):
        """Take all compressed chunks in memory and save all the consecutive ones at the beginning."""

        i = self.index_write
        while True:
            if i in self.compressed_chunks:
                self.write_chunk(self.compressed_chunks[i])
                del self.compressed_chunks[i]
                i += 1
            else:
                break

        self.index_write = i

    def write_chunk(self, chunk):
        """Write chunk to disk."""
        
        self.fout.write(chunk)


# Code:
if __name__ == "__main__":
    main()
