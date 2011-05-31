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
import subprocess as sp
import optparse
import chopzip.core as core

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

# If number of cores not given, use them all:
if not o.ncpus:
    o.ncpus = core.count_cores()

if o.timing:
    import Time as T
    t = T.Timing()

if o.decompress:
    for fn in args:
        # Check that file exists:
        core.isfile(fn)
        
        # If not defined explicitly, guess format by extension:
        if not o.method:
            o.method = core.guess_by_ext(fn)
            
        # Create main object:
        cc = core.Compression(o)
                
        # Decompress:
        cc.decompress(fn)
        
        if o.timing: 
            t.milestone('Decompressed {0}'.format(fn))
            
        if o.timing:
            t.milestone('Ended')
            print(t.summary())

else:
    for fn in args:
        # Check that file exists:
        core.isfile(fn)
        
        # Default method if none specified:
        if not o.method: 
            o.method = 'xz'
            
        # Create main object:
        cc = core.Compression(o)
        
        # Split in ncpu chunks:
        chunks = core.split_it(fn,o)
        
        if o.timing:
            t.milestone('Chopped {0}'.format(fn))
            
        # Compress:
        cc.compress_chunks(chunks)

        if o.timing:
            t.milestone('Compressed chunks of {0}'.format(fn))

        # Join chunks:
        cc.join_chunks( chunks, fn)
        
        if o.timing: 
            t.milestone('Joined chunks of {0}'.format(fn))
            
        # Remove uncompressed:
        os.unlink(fn)
        
        # Remove tmp:
        for chunk in chunks:
            os.unlink(chunk+'.'+cc.ext)
        
        if o.timing:
            t.milestone('Ended')
            print t.summary()

