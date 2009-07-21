#!/usr/bin/python

import os
import sys
import math
import glob
import subprocess
import optparse

#--------------------------------------------------------------------------------#

# Read arguments:
parser = optparse.OptionParser()

parser.add_option("-d","--decompress",
                  action  = "store_true",
                  help    = "Decompress file. Default: compress.",
		  default = False)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

ncpu  = 4
sp    = subprocess.Popen

if o.decompress:

  for fn in args:

    # Untar:
    cmnd = 'tar -xvf %s' % (fn)
    p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
    p.wait()

    # Decompress:
    files = p.stdout.readlines()

    for file in files:

      file = file.replace('\n','')
      cmnd = 'unlzma -S .lz %s' % (file)
      p = sp(cmnd,shell=True)
      p.wait()

    # Join parts:
    cmnd = 'cat '
    for file in files:
      file = file.replace('.lz\n','')
      cmnd += '%s ' % (file)

    out = fn.replace('.plz','')
    
    cmnd += ' > %s' % (out)

    p = sp(cmnd,shell=True)
    p.wait()

    # Remove tmp:
    for file in files:
      file = file.replace('.lz\n','')
      os.unlink(file)

    # Remove TAR:
    os.unlink(fn)

else:

  for fn in args:

    # Split in ncpu chunks:
    total_size = os.path.getsize(fn)
    chunk_size = math.trunc(total_size/ncpu+1)
    cmnd       = 'split -b %i -d %s %s.' % (chunk_size,fn,fn)
    p          = sp(cmnd,shell=True)
    p.wait()

    chunks = glob.glob('%s.[0-9]*' % (fn))

    pchunk = {}
    for chunk in chunks:
  
      cmnd = 'lzma -S .lz -3 "%s"' % (chunk)
      pchunk[chunk] = sp(cmnd,shell=True)

    # Wait for all processes to finish:
    for k,v in pchunk.items():
      v.wait()

    # TAR result:
    cmnd = 'tar -cf %s.plz ' % (fn) + '.lz '.join(chunks) + '.lz'
    p = sp(cmnd,shell=True)
    p.wait()
    
    # remove tmp:
    for chunk in chunks:
      os.unlink(chunk+'.lz')

    # Remove uncompressed:
    os.unlink(fn)
