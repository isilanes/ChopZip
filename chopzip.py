#!/usr/bin/python2
# coding=utf-8

'''
ChopZip
(c) 2009-2010, Iñaki Silanes

LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License (version 2 or later),
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details (http://www.gnu.org/licenses/gpl.txt).

DESCRIPTION

(De)compresses files with LZMA/gzip/xz/lzip in parallel.

USAGE

% chopzip [options] file(s)

for options:

% chopzip -h 
'''

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
                  help    = "Number of CPUs to use. Default: autodetect number of cores.",
		  type    = 'int',
                  default = None)

parser.add_option("-l","--level",
                  help    = "Compression level (1 min to 9 max). Default: 3.",
		  type    = 'int',
                  default = 3)

parser.add_option("-m","--method",
                  help    = "Compression method. Available: gzip, lzma, xz. Default: xz to compress, select by extension to decompress.",
		  type    = 'str',
                  default = None)

parser.add_option("-T", "--timing",
                  action="store_true",
                  help="Ask for timing of various steps. Default: don't.",
		  default=False)

parser.add_option("-v", "--verbose",
                  action="store_true",
                  help="Be extra verbose. Default: don't be.",
		  default=False)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

def mysplit(fn,nchunks=1):
    
    chunks = []
    
    total_size = os.path.getsize(fn)
    chunk_size = math.trunc(total_size/nchunks) + 1
    cmnd       = 'split --verbose -b {0} -a 3 -d "{1}" "{1}.chunk."'.format(chunk_size,fn)
    if o.verbose: print cmnd
    p = sp(cmnd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p.wait()
    
    for line in p.stdout.readlines():
        line  = line.replace("'",'')
        line  = line.replace("\n",'')
        chunk = line.split('`')[-1]
        chunks.append(chunk)
        
    return chunks

#--------------------------------------------------------------------------------#

def chkfile(fn):
    
    if not os.path.isfile(fn):
        msg = 'Error: you requested operation on file "%s", but I can not find it!' % (fn)
        sys.exit(msg)

#--------------------------------------------------------------------------------#

def ends(string,substring):
    
    nc = len(substring)
    ending = ''.join(string[-nc:])
    
    if substring == ending:
        return True
    else:
        return False

#--------------------------------------------------------------------------------#

#
# The dictionary here defines all that needs to be know about a compressor:
#
# cat : whether concatenated compressed files are fine. If not, tar must be used.
# ext : extension of compressed file
# tax : extension of tarred file, if tarred.
# com : compression command
# dec : decompression command
#

methods = { 
            'xz' : {
                     'cat' : True,
                     'ext' : 'xz',
	             'com' : 'xz',
                     'dec' : 'xz -d',
                    },

            'gzip' : {
                     'cat' : True,
                     'ext' : 'gz',
	             'com' : 'gzip',
                     'dec' : 'gzip -d',
                    },

            'lzip' : {
                     'cat' : True,
                     'ext' : 'lz',
	             'com' : 'lzip',
                     'dec' : 'lzip -d',
                    },
            'lzma' : {
                     'cat' : False,
                     'ext' : 'lzma',
                     'tax' : 'plzma',
	             'com' : 'lzma',
                     'dec' : 'lzma -d',
                    },
          }

#--------------------------------------------------------------------------------#

if o.method and not o.method in methods:
    msg = 'Unknown compression method "{0}" requested'.format(o.method)
    sys.exit(msg)

if not o.ncpus:
    fn = '/proc/cpuinfo'
    f = open(fn)
    
    o.ncpus = 0
    for line in f:
        if 'processor	:' in line:
            o.ncpus += 1

if o.timing:
    import Time as T
    t = T.Timing()

sp = subprocess.Popen

if o.decompress:
  for fn in args:

    chkfile(fn)

    # If not defined explicitly, guess format by extension:
    if not o.method:
      for k,v in methods.items():
          try:
              if ends(fn,'.'+v['tax']): 
                  o.method = k
                  break
          except:
              pass

      for k,v in methods.items():
          if ends(fn,'.'+v['ext']): 
              o.method = k
              break

      # If still no match, die:
      if not o.method:
          msg = 'Don\'t know how "{0}" was compressed'.format(fn)
          sys.exit(msg)

    # Dictionary with details:
    m = methods[o.method]

    # Decompress:

    if m['cat']:
        # Then simple concatenation can be (and was) used in compression.
        cmnd = '{0} {1}'.format(m['dec'], fn)
        if o.verbose: print cmnd
        p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
        p.communicate()

    else:
      # Then tar must have been used.

      basefn = fn.replace('.'+m['tax'],'')

      # First, untar:
      cmnd = 'tar -xf {0}'.format(fn)
      if o.verbose: print cmnd
      p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
      p.wait()
      chunks = glob.glob('{0}.chunk.*'.format(basefn))

      # Then, decompress each chunk:
      conc = 'cat '
      for chunk in chunks:
          cmnd = '{0} {1}'.format(m['dec'], chunk)
          if o.verbose: print cmnd
          p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
          p.wait()
          conc += ' {0} '.format(chunk.replace('.'+m['ext'],''))
      conc += ' > {0}'.format(basefn)

      # Then, concatenate uncompressed chunks:
      p = sp(conc,shell=True,stdout=subprocess.PIPE)
      p.wait()

      # Finally, delete chunks and tarred file:
      os.remove(fn)
      for chunk in chunks:
          os.remove(chunk.replace('.'+m['ext'],''))

    if o.timing: t.milestone('Decompressed {0}'.format(fn))

    if o.timing:
        t.milestone('Ended')
        print t.summary()

else:
  for fn in args:
    chkfile(fn)

    # Default method if none specified:
    if not o.method: o.method = 'xz'

    # Dictionary with details:
    m = methods[o.method]

    # Split in ncpu chunks:
    chunks = mysplit(fn,o.ncpus)

    if o.timing: t.milestone('Chopped {0}'.format(fn))

    # Create one compression thread per chunk:
    pd  = []
    for chunk in chunks:
        cmnd = '{0} -{1} "{2}"'.format(m['com'], int(o.level), chunk)
        if o.verbose: print cmnd
        pd.append(sp(cmnd,shell=True))

    # Wait for all processes to finish:
    for p in pd:
        p.wait()

    if o.timing:
        t.milestone('Compressed chunks of {0}'.format(fn))

    # Join chunks:

    if m['cat']:
        # Then simple concatenation can be used.
        cmnd = 'cat '
        for chunk in chunks:
            cmnd += ' "{0}.{1}" '.format(chunk,m['ext'])
        cmnd += ' > "{0}.{1}"'.format(fn,m['ext'])
        if o.verbose: print cmnd
        p = sp(cmnd,shell=True)
        p.wait()

    else:
        # Then tar must be used.
        cmnd = 'tar -cf "{0}.{1}" '.format(fn, m['tax'])
        for chunk in chunks:
            cmnd += ' "{0}.{1}" '.format(chunk,m['ext'])
        if o.verbose: print cmnd
        p = sp(cmnd,shell=True)
        p.wait()

    if o.timing: t.milestone('Joined chunks of {0}'.format(fn))

    # Remove uncompressed:
    os.unlink(fn)

    # Remove tmp:
    for chunk in chunks:
        os.unlink(chunk+'.'+m['ext'])

    if o.timing:
        t.milestone('Ended')
        print t.summary()

