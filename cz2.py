# Standard libs:
import lzma
import time
import pickle
import multiprocessing as mp

# Functions:
def main():
    """Main function."""

    fn_in = "somefile"
    fn_out = fn_in + ".xz"

    with open(fn_out, "w") as fout:
        with open(fn_in, 'rb') as fin:
            CZ = ChopZip(fin, fout)
            CZ.run()


# Classes:
class ChopZip(object):
    """Class with all methods."""

    CHUNK_SIZE = 10*1024*1024
    NPROCS = 4

    def __init__(self, fin, fout):
        self.fin = fin
        self.fout = fout
        self.procs = []
        self.index_read = 0
        self.index_write = 0
        self.compressed_chunks = {}

    def chunk_read(self):
        """Iterable over data chunks read from input."""

        while True:
            data = self.fin.read(self.CHUNK_SIZE)
            if not data:
                break
            yield data

    def compress_chunk(self, chunk, i, lock):
        """Take a single data chunk and compress it."""

        compressed_chunk = lzma.compress(chunk)
        lock.acquire()
        try:
            with open("tmp", "rb") as f:
                d = pickle.load(f)
        except:
            d = {}

        d[i] = compressed_chunk
        with open("tmp", "w") as f:
            pickle.dump(d, f)

        lock.release()
        print("chunk compressed:", i, [x for x  in d])

    def run(self):
        """Run the whole thing."""

        # Create a lock for using when no concurrency is needed:
        self.lock = mp.Lock()

        while True:
            # Feed a new process if pool low:
            self.feed_compression_queue_if_low()

            # Serial stuff:
            self.lock.acquire()
            print("cycle", [i for i in self.compressed_chunks])

            # Remove completed processes from process list:
            self.clean_proc_list()

            # Try to save to disk:
            self.save_ordered_chunks_to_disk()

            self.lock.release()

            time.sleep(1.0)

            # When to exit:
            if self.job_is_done:
                break

    def feed_compression_queue_if_low(self):
        """Feed compression queue, if low."""

        chunk = self.chunk_read()
        if chunk and len(self.procs) < self.NPROCS:
            self.feed_chunk_to_compress_queue(chunk)

    def feed_chunk_to_compress_queue(self, chunk):
        """Feed given data chunk to compression loop."""

        p = mp.Process(target=self.compress_chunk, args=(chunk, self.index_read, self.lock))
        p.daemon = True
        p.start()
        self.procs.append(p)
        print("chunk read, index:", self.index_read)
        self.index_read += 1

    def clean_proc_list(self):
        """Remove completed processes from process list."""

        self.procs = [p for p in self.procs if p.is_alive()]

    def save_ordered_chunks_to_disk(self):
        """Take all compressed chunks in memory and save all the consecutive ones at the beginning."""

        i = self.index_write
        while True:
            if i in self.compressed_chunks:
                self.fout.write(self.compressed_chunks[i])
                del self.compressed_chunks[i]
                i += 1
            else:
                break

        self.index_write = i

    @property
    def job_is_done(self):
        """Return True if job is done, False otherwise."""

        # If any compression still running, job not done:
        if self.procs:
            return False
        
        # If input file still not fully read, job not done:
        #return False

        # If any chunk in memory still not written, job not done:
        if self.compressed_chunks:
            return False

        # Else, return True:
        return True



# Code:
if __name__ == "__main__":
    main()
