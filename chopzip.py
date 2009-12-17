#!/usr/bin/python
# coding=utf-8

'''
ChopZip
(c) 2009, Iñaki Silanes

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
                  help    = "Number of CPUs to use. Default: 2.",
		  type    = 'int',
                  default = 2)

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

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

def mysplit(fn,nchunks=1):

  chunks = []

  total_size = os.path.getsize(fn)
  chunk_size = math.trunc(total_size/nchunks) + 1
  cmnd       = 'split --verbose -b %i -a 3 -d %s %s.chunk.' % (chunk_size,fn,fn)
  p          = sp(cmnd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  p.wait()

  for line in p.stdout.readlines():
    line = line.replace("'",'')
    line = line.replace("`",'')
    aline = line.split()[-1]
    chunks.append(aline)

  return chunks

#--------------------------------------------------------------------------------#

def chkfile(fn):

  if not os.path.isfile(fn):

    msg = 'Error: you requested operation on file "%s", but I can not find it!' % (fn)
    sys.exit(msg)

#--------------------------------------------------------------------------------#

def ends(string,substring):

  nc     = len(substring)
  ending = ''.join(string[-nc:])

  if substring == ending:
    return True

  else:
    return False

#--------------------------------------------------------------------------------#

if o.method and not o.method in ['xz','lzma','gzip','lzip']:

  msg = 'Unknown compression method "{0}" requested'.format(o.method)
  sys.exit(msg)

if o.timing:
  import Time as T
  t = T.timing()

sp = subprocess.Popen

if o.decompress:

  for fn in args:

    chkfile(fn)

    if not o.method:
      if   ends(fn,'.lz'):   o.method = 'lzip'
      elif ends(fn,'.lzma'): o.method = 'lzma'
      elif ends(fn,'.xz'):   o.method = 'xz'
      elif ends(ffn,'.gz'):  o.method = 'gzip'
      else:
        msg = 'Don\'t know how "{0}" was compressed'.format(file)
	sys.exit(msg)

    # Take advantage of the fact that the method name 
    # is equal to the command name:
    cmnd = '{0} -d {1}'.format(o.method,fn)

    p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
    p.wait()

    if o.timing:
      t.milestone('Decompressed {0}'.format(fn))

    if o.timing:
      t.milestone('Ended')
      print t.summary()

else:

  for fn in args:

    chkfile(fn)

    if not o.method:
      o.method = 'xz'

    # Split in ncpu chunks:
    chunks = mysplit(fn,o.ncpus)

    if o.timing:
      t.milestone('Chopped {0}'.format(fn))

    pd  = []
    ext = 'xz'
    for chunk in chunks:

      if o.method == 'lzma':
        cmnd = 'lzma -%i "%s"' % (int(o.level),chunk)

      elif o.method == 'xz':
        ext   = 'xz'
        cmnd  = 'xz -%i "%s"' % (int(o.level),chunk)

      elif o.method == 'gzip':
        ext   = 'gz'
        cmnd  = 'gzip -%i "%s"' % (int(o.level),chunk)

      elif o.method == 'lzip':
        ext  = 'lz'
        cmnd = 'lzip -%i "%s"' % (int(o.level),chunk)

      pd.append(sp(cmnd,shell=True))

    # Wait for all processes to finish:
    for p in pd:
      p.wait()

    if o.timing:
      t.milestone('Compressed chunks of {0}'.format(fn))

    # Join chunks:
    cmnd = 'cat '

    for chunk in chunks:
      cmnd += ' {0}.{1} '.format(chunk,ext)

    cmnd += ' > {0}.{1}'.format(fn,ext)
    p     = sp(cmnd,shell=True)
    p.wait()

    if o.timing:
      t.milestone('Joined chunks of {0}'.format(fn))

    # Remove uncompressed:
    os.unlink(fn)

    # remove tmp:
    for chunk in chunks:
      os.unlink(chunk+'.'+ext)

    if o.timing:
      t.milestone('Ended')
      print t.summary()

