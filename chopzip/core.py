import os
import sys
import math
import subprocess as sp

#------------------------------------------------------------------------------#

def split_it(fn,opts):
    '''
    Function that takes a "fn" file name and makes "opts.ncpus" equal pieces 
    out of it. It returns the list of filenames for the chunks.
    '''
    
    chunks = []
    
    total_size = os.path.getsize(fn)
    chunk_size = math.trunc(total_size/opts.ncpus) + 1
    cmnd = 'split --verbose -b {0} -a 3 -d "{1}" "{1}.chunk."'.format(chunk_size, fn)
    if opts.verbose:
        print(cmnd)
    p = sp.Popen(cmnd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    s = p.communicate()
    
    m = str(s[0],encoding='utf8').split('\n')[:-1]

    for line in m:
        line = line.replace("'",'')
        line = line.replace("\n",'')
        chunk = line.split('`')[-1]
        chunks.append(chunk)
        
    return chunks

#------------------------------------------------------------------------------#

class Compression:
    '''
    This class holds vars and methods for compression.
    '''

    m = { 
            'xz' : {
                'ext' : 'xz',
                'com' : 'xz',
                'dec' : 'xz -d',
                }
            }


    def __init__(self,opts):
        try:
            method = opts.method
            self.ext = self.m[method]['ext']
            self.com = self.m[method]['com']
            self.dec = self.m[method]['dec']
        except:
            msg = 'Unknown compression method "{0}" requested'.format(opts.method)
            sys.exit(msg)

        self.o = opts

    # ----- #

    def decompress(self, fn):
        '''
        Method to decompress given file "fn".
        '''

        cmnd = '{0} "{1}"'.format(self.dec, fn)
        if self.o.verbose:
            print(cmnd)

        p = sp.Popen(cmnd, stdout=sp.PIPE, shell=True)
        p.communicate()

    # ----- #

    def compress_chunks(self, chunks):
        '''
        Method to compress a given file in parallel, by compressing its
        chunks "chunks".
        '''

        # Create one compression thread per chunk:
        pd  = []

        for chunk in chunks:
            cmnd = '{0} -{1} "{2}"'.format(self.com, int(self.o.level), chunk)
            if self.o.verbose:
                print(cmnd)

            pd.append(sp.Popen(cmnd,shell=True))
        
        # Wait for all processes to finish:
        for p in pd:
            p.communicate()

    # ----- #

    def join_chunks(self, chunks, fn):
        '''
        Method to join given chunks "chunks" into given file "fn".
        '''

        cmnd = 'cat '
        for chunk in chunks:
            cmnd += ' "{0}.{1}" '.format(chunk, self.ext)

        cmnd += ' > "{0}.{1}"'.format(fn, self.ext)

        if self.o.verbose:
            print(cmnd)

        p = sp.Popen(cmnd,shell=True)
        p.communicate()

#------------------------------------------------------------------------------#

def count_cores():
    '''
    Function to count the number of available cores, to use them all if no
    amount is defined by user.
    '''

    ncores = 0
    fn = '/proc/cpuinfo'

    f = open(fn)
    for line in f:
        if 'processor	:' in line:
            ncores += 1
    f.close()

    return ncores

#------------------------------------------------------------------------------#

def guess_by_ext(fn):
    '''
    Take the file name "fn" of an alleged compressed file, and guess 
    the compression method by the extension.
    '''

    method = None

    for k,v in Compression.m.items():
        if ends(fn,'.'+v['ext']):
            method = k
            break

    if method:
        return method
    else:
        msg = 'Don\'t know how "{0}" was compressed'.format(fn)
        sys.exit(msg)

#------------------------------------------------------------------------------#

def ends(string,substring):
    '''
    Returns True if "string" ends in "substring", and False otherwise.
    '''
    
    nc = len(substring)
    ending = ''.join(string[-nc:])
    
    if substring == ending:
        return True
    else:
        return False

#------------------------------------------------------------------------------#

def isfile(fn):
    '''
    Die if file "fn" is not present.
    '''

    if not os.path.isfile(fn):
        fmt = 'Error: you requested operation on file "%s", but I can not find it!'
        msg = fmt % (fn)
        sys.exit(msg)
