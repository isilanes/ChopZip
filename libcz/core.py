import os
import sys
import math
import time
import subprocess as sp

#------------------------------------------------------------------------------#

def split_it(fn, opts):
    ''' Function that takes a "fn" file name and makes "opts.ncores" equal pieces 
    out of it. If opts.chunksize is given, make chunks of that size instead.
    
    It returns the list of filenames for the chunks.'''

    ocd = opts.chunk_dir

    # Check if the tmp dir for chunks exists. If it doesn't, create it, and
    # remind us later that we should delete it.
    delete_tmpdir = False
    if not os.path.isdir(ocd):
        try:
            os.makedirs(ocd)
            delete_tmpdir = True
        except:
            msg = 'Error: could not create tmp dir "{0}". Exiting...'.format(ocd)
            print(msg)
            sys.exit()
    
    # The list of chunks:
    chunks = []
    
    # Calculate size of each chunk:
    if opts.chunksize:
        chunk_size = opts.chunksize
    else:
        total_size = os.path.getsize(fn)
        chunk_size = math.trunc(total_size/opts.ncores) + 1

    # Use the "split" command to make actual splitting:
    fmt = 'split --verbose -b {0} -a 3 -d "{1}" "{2}/{1}.chunk."'
    cmnd = fmt.format(chunk_size, fn, ocd)
    if opts.verbose:
        print(cmnd)
    p = sp.Popen(cmnd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    s = p.communicate()
    
    # Get the output of split (list of chunk filenames):
    m = str(s[0], encoding='utf8').split('\n')[:-1]

    # Create list of chunks from the output of split:
    for line in m:
        line = line.replace("'",'')
        line = line.replace("\n",'')
        chunk = line.split('`')[-1]
        chunks.append(chunk)
        
    return chunks, delete_tmpdir

#------------------------------------------------------------------------------#

class Compression:
    '''This class holds vars and methods for compression.'''

    m = { 
            'xz' : {
                'ext' : 'xz',   # extension of compressed files, as in "blah.ext"
                'com' : 'xz',   # name of compressing command
                'dec' : 'xz -d',# name of decompressing command
                }
            }


    def __init__(self, opts):
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
        ''' Method to decompress given file "fn".'''

        cmnd = '{0} {2} "{1}"'.format(self.dec, fn, self.o.command_args)
        if self.o.verbose:
            print(cmnd)

        p = sp.Popen(cmnd, stdout=sp.PIPE, shell=True)
        p.communicate()

    # ----- #

    def compress_chunks(self, chunks):
        '''Method to compress a given file in parallel, by compressing its
        chunks "chunks".'''

        # Create a list of remaining (uncompressed) chunks:
        remaining = chunks[:]

        # Create a list of compression threads, up to opts.ncores, and
        # fill it with remaining chunks as threads are finished, while
        # chunks remain in remaining:
        pd  = []
        while len(pd) < self.o.ncores or remaining:
            # Pop a new chunk into new compression thread, if less than opts.ncores:
            if len(pd) < self.o.ncores and remaining:
                current = remaining.pop()
                cmnd = '{0} -{1.level} {1.command_args} "{2}"'.format(self.com, self.o, current)
                if self.o.verbose:
                    print(cmnd)
                pd.append(sp.Popen(cmnd,shell=True))

            # Check if any thread finished, and take it out of the thread list if so:
            new_pd = []
            for p in pd:
                finished = p.poll()
                if finished == None:
                    new_pd.append(p)
                    time.sleep(0.1)
            pd = new_pd[:]

            # If done, quit:
            if not pd and not remaining:
                break

            # Sleep before next cycle:
            time.sleep(0.1)

    # ----- #

    def join_chunks(self, chunks, fn):
        '''Method to join given chunks "chunks" into given file "fn".'''

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
    '''Function to count the number of available cores, to use them all if no
    amount is defined by user. Default to 1 if we could not retrieve the info.'''

    ncores = 0
    fn = '/proc/cpuinfo'

    try:
        with open(fn,'r') as f:
            for line in f:
                if 'processor	:' in line:
                    ncores += 1
    except:
        ncores = 1

    return ncores

#------------------------------------------------------------------------------#

def guess_by_ext(fn):
    '''Take the file name "fn" of an alleged compressed file, and guess 
    the compression method by the extension.'''

    method = None

    for k,v in Compression.m.items():
        if ends(fn,'.'+v['ext']):
            method = k
            break

    if method:
        return method
    else:
        msg = 'Sorry, don\'t know how "{0}" was compressed'.format(fn)
        sys.exit(msg)

#------------------------------------------------------------------------------#

def ends(string, substring):
    '''Returns True if "string" ends in "substring", and False otherwise.'''
    
    nc = len(substring)
    ending = ''.join(string[-nc:])
    
    if substring == ending:
        return True
    else:
        return False

#------------------------------------------------------------------------------#

def isfile(fn):
    '''Die if file "fn" is not present.'''

    if not os.path.isfile(fn):
        fmt = 'Error: you requested operation on file "{0}", but I can not find it!'
        msg = fmt.format(fn)
        sys.exit(msg)

#------------------------------------------------------------------------------#

class Timing:
  
  def __init__(self):
      self.t0 = time.time()
      self.milestones = []
      self.data = {}

  # --- #

  def milestone(self,id=None):
      '''Define a milestone.'''

      # ID of milestone:
      if not id:
          id = 'unk'

      # Avoid dupe IDs:
      while id in self.milestones:
          id += 'x'

      # Current time:
      tnow = time.time()

      # Log data:
      self.milestones.append(id)
      self.data[id] = { 'time' : tnow }

  # --- #

  def summary(self,seconds=True):
      '''Print a summary of milestones so far.'''

      # Time of last milestone so far:
      otime = self.t0

      # Max with of labels, in characters:
      maxl = 9
      for milestone in self.milestones:
          l = len(milestone) + 1
          if l > maxl:
              maxl = l

      # Header:
      smry = '\n{0:>8} {1:>{3}} {2:>8}\n'.format('Time', 'Milestone', 'Elapsed', maxl)

      # Print a line for each milestone:
      for milestone in self.milestones:
          # Elapsed since last milestone (aka "otime"):
          t = self.data[milestone]['time']
          delta =  t - otime
          otime = t

          smry += '{0:>9.2f} {1:>{3}} {2:>9.2f}\n'.format(t-self.t0, milestone, delta, maxl)

      return smry

#--------------------------------------------------------------------------------#
