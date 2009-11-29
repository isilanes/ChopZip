#!/usr/bin/python
# coding=utf-8

'''
plzma
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

% plzma [options] file(s)

for options:

% plzma -h 
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
                  help    = "Compression method. Available: gzip, lzma, xz. Default: lzma.",
		  type    = 'str',
                  default = 'lzma')

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

def mysplit(fn,nchunks=2):

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

if not o.method in ['xz','lzma','gzip','lzip']:

  msg = 'Unknown compression method "{0}" requested'.format(o.method)
  sys.exit(msg)

sp = subprocess.Popen

if o.decompress:

  for fn in args:

    chkfile(fn)

    if ends(fn,'.lz'):
      o.method = 'lzip'

    if o.method == 'lzip':
      cmnd = 'lzip -d {0}'.format(fn)
      p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
      p.wait()

    else:
      # Untar:
      cmnd = 'tar -xvf %s' % (fn)
      p = sp(cmnd,shell=True,stdout=subprocess.PIPE)
      p.wait()

      # Decompress:
      files = p.stdout.readlines()

      pd = []
      for file in files:

        file = file.replace('\n','')

        if ends(file,'.lzma'):
          cmnd = 'lzma -d %s' % (file)
          pd.append(sp(cmnd,shell=True))

        elif ends(file,'.gz'):
          cmnd = 'gzip -d %s' % (file)
          pd.append(sp(cmnd,shell=True))

        elif ends(file,'.xz'):
          cmnd = 'xz -d %s' % (file)
          pd.append(sp(cmnd,shell=True))

        else:
          print 'Don\'t know how "{0}" was compressed'.format(file)

      for p in pd:
        p.wait()

      # Join parts:
      cmnd = 'cat '
      for file in files:
        afile = file.split('.')
        file = '.'.join(afile[:-1])
        cmnd += '%s ' % (file)

      aout = fn.split('.')
      out  = '.'.join(aout[:-1])
    
      cmnd += ' > %s' % (out)

      p = sp(cmnd,shell=True)
      p.wait()

      # Remove tmp:
      for file in files:
        afile = file.split('.')
        file  = '.'.join(afile[:-1])
        os.unlink(file)

      # Remove TAR:
      os.unlink(fn)

else:

  for fn in args:

    chkfile(fn)

    # Split in ncpu chunks:
    chunks = mysplit(fn,o.ncpus)

    pd   = []
    ext  = 'lzma'
    text = 'plz'
    for chunk in chunks:

      if o.method == 'lzma':
        cmnd = 'lzma -%i "%s"' % (int(o.level),chunk)
        pd.append(sp(cmnd,shell=True))

      elif o.method == 'xz':
        ext   = 'xz'
        text  = 'pxz'
        cmnd  = 'xz -%i "%s"' % (int(o.level),chunk)
        pd.append(sp(cmnd,shell=True))

      elif o.method == 'gzip':
        ext   = 'gz'
        text  = 'pgz'
        cmnd  = 'gzip -%i "%s"' % (int(o.level),chunk)
        pd.append(sp(cmnd,shell=True))

      elif o.method == 'lzip':
        ext  = 'lz'
        cmnd = 'lzip -%i "%s"' % (int(o.level),chunk)
        pd.append(sp(cmnd,shell=True))

    # Wait for all processes to finish:
    for p in pd:
      p.wait()

    if o.method == 'lzip':
      cmnd = 'cat '

      for chunk in chunks:
        cmnd += ' {0}.{1} '.format(chunk,ext)

      cmnd += ' > {0}.lz'.format(fn)
      p     = sp(cmnd,shell=True)
      p.wait()

    else:

      # TAR result:
      pext = '.'+ext+' '
      cmnd = 'tar -cf %s.%s ' % (fn,text) + pext.join(chunks) + pext
      p    = sp(cmnd,shell=True)
      p.wait()
    
    # Remove uncompressed:
    os.unlink(fn)

    # remove tmp:
    for chunk in chunks:
      os.unlink(chunk+'.'+ext)

