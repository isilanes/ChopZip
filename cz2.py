import lzma
import time
import pickle
import multiprocessing as mp

class ChopZip(object):

    def __init__(self, fin, fout):
        self.chunk_size = 10*1024*1024
        self.nprocs = 4
        self.fin = fin
        self.fout = fout
        self.procs = []
        self.index_read = 0
        self.index_write = 0
        self.compressed_chunks = {}

    def chunk_read(self):
        while True:
            data = self.fin.read(self.chunk_size)
            if not data:
                break
            yield data

    def compress_chunk(self, chunk, i, lock):
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
        print "chunk compressed:", i, [ x for x  in d ]

    def run(self):
        self.lock = mp.Lock()

        for chunk in self.chunk_read():
            # Feed a new process if pool low:
            if len(self.procs) < self.nprocs:
                p = mp.Process(target=self.compress_chunk, args=(chunk, self.index_read, self.lock))
                p.daemon = True
                p.start()
                self.procs.append(p)
                print "chunk read, index:", self.index_read
                self.index_read += 1

            self.lock.acquire()
            try:
                with open("tmp", "rb") as f:
                    self.compressed_chunks = pickle.load(f)
            except:
                self.compressed_chunks = {}
            print "cycle", [ i for i in self.compressed_chunks ]

            # Remove completed processes:
            alive = []
            for p in self.procs:
                if p.is_alive():
                    alive.append(p)
            self.procs = alive[:]

            # Try to save to disk:
            alive = {}
            for i,chunk in self.compressed_chunks.items():
                print i
                if i == self.index_write:
                    self.fout.write(chunk)
                    self.index_write += 1
                    print self.index_write
                else:
                    alive[i] = chunk
            self.compressed_chunks = alive.copy()

            with open("tmp", "w") as f:
                pickle.dump(self.compressed_chunks, f)
            self.lock.release()

            time.sleep(1.0)
            #if len(self.procs) == self.nprocs:
            #    time.sleep(10.0)


fn_in = "somefile"
fn_out = fn_in + ".xz"

with open(fn_out, "w") as fout:
    with open(fn_in, 'rb') as fin:
        CZ = ChopZip(fin, fout)
        CZ.run()
