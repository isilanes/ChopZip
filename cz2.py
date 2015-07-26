import lzma

def chunk_read(f, chunk_size=1024*1024):
    while True:
        data = f.read(chunk_size)
        if not data:
            break
        yield data


data_out = b''
fin = "somefile"
with open(fin, 'rb') as f:
    for piece in chunk_read(f):
        data_out += lzma.compress(piece)

fout = fin + ".xz"
with open(fout, "w") as f:
    f.write(data_out)
