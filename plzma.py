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

parser.add_option("-n","--ncpus",
                  help    = "Number of CPUs to use. Default: 2.",
                  default = 2)

parser.add_option("-l","--level",
                  help    = "Compression level (1 min to 9 max). Default: 3.",
                  default = 3)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

sp    = subprocess.Popen

if o.decompress:

  for fn in args:

    # Untar:
    cmnd = 'tar -xvf %s' % (fn)
    p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
    p.wait()

    # Decompress:
    files = p.stdout.readlines()

    pd = []
    for file in files:

      file = file.replace('\n','')
      cmnd = 'lzma -d %s' % (file)
      pd.append(sp(cmnd,shell=True))

    for p in pd:
      p.wait()

    # Join parts:
    cmnd = 'cat '
    for file in files:
      file = file.replace('.lzma\n','')
      cmnd += '%s ' % (file)

    out = fn.replace('.plz','')
    
    cmnd += ' > %s' % (out)

    p = sp(cmnd,shell=True)
    p.wait()

    # Remove tmp:
    for file in files:
      file = file.replace('.lzma\n','')
      os.unlink(file)

    # Remove TAR:
    os.unlink(fn)

else:

  for fn in args:

    # Split in ncpu chunks:
    total_size = os.path.getsize(fn)
    chunk_size = math.trunc(total_size/int(o.ncpus)+1)
    cmnd       = 'split -b %i -d %s %s.' % (chunk_size,fn,fn)
    p          = sp(cmnd,shell=True)
    p.wait()

    chunks = glob.glob('%s.[0-9]*' % (fn))

    pd = []
    for chunk in chunks:
  
      cmnd = 'lzma -%i "%s"' % (int(o.level),chunk)
      pd.append(sp(cmnd,shell=True))

    # Wait for all processes to finish:
    for p in pd:
      p.wait()

    # TAR result:
    cmnd = 'tar -cf %s.plz ' % (fn) + '.lzma '.join(chunks) + '.lzma'
    p    = sp(cmnd,shell=True)
    p.wait()
    
    # remove tmp:
    for chunk in chunks:
      os.unlink(chunk+'.lzma')

    # Remove uncompressed:
    os.unlink(fn)
