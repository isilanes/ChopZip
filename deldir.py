#!/usr/bin/python
# coding=utf-8

'''
deldir.py
(c) 2008, Iñaki Silanes

LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License (version 2), as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
for more details (http://www.gnu.org/licenses/gpl.txt).

DESCRIPTION

Deletes dirs at backup.dreamhost.com (shitty rssh shell won't allow SSH,
and gFTP fails at deleting big dirs).

USAGE

For usage, run:

% deldir.py -h

VERSION

svn_revision = r1

'''

import datetime
import glob
import optparse
import os
import re
import sys
import operator

sys.path.append(os.environ['HOME']+'/WCs/PythonModules')

import Private as P
import System as S
import FileManipulation as FM

#--------------------------------------------------------------------------------#

# Read arguments:
parser = optparse.OptionParser()

parser.add_option("-d", "--date",
                  help="Date of dir to del, in YYYY.MM.DD format. Default: none.",
                  default=None)

parser.add_option("-v", "--verbose",
                  dest="verbosity",
                  help="Increase verbosity level by 1 (0 = no output, 1 = print commands being executed, 2 = print summary too, 3 = print transferred files too, 4 = print progress meter too). Default: 0.",
		  action="count",
                  default=0)

parser.add_option("-y", "--dryrun",
                  help="Dry run: do nothing, just tell what would be done. Default: real run.",
		  action="store_true",
                  default=False)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

# Make dry runs verbose:
if o.dryrun:
  o.verbosity += 1

#--------------------------------------------------------------------------------#

def doit(cmnd=None):
  '''
  Print and/or execute command, depending on o.verbosity and o.dryrun.
    cmnd = command to run
  '''

  if o.verbosity > 0:
    print cmnd

  if not o.dryrun:
    S.cli(cmnd)

#--------------------------------------------------------------------------------#

if not o.date:
  sys.exit('Please, specify dir to delete (in YYYY.MM.DD format).')

# Variables:
ddir    = '/home/b395676/backups/flanders.home_%s' % (o.date)
machine = 'b395676@backup.dreamhost.com'
remote  = '%s:%s' % (machine,ddir)
rsync   = 'rsync --delete -rv --protocol=29'

# Make dummy dir:
dummydir = 'dummy'
# Try succesive names, until file/dir so named does not exist:
while os.path.exists(dummydir):
  dummydir += '-blah'

if not o.dryrun:
  os.mkdir(dummydir)

# Sync it, to obliterate remote:
cmnd = '%s %s/ %s/' % (rsync, dummydir, remote)
doit(cmnd)

# Delete (now empty) remote dir:
cmnd = 'echo "rmdir %s/" | sftp -b - %s' % (ddir, machine)
doit(cmnd)

# Remove tmp dummy dir:
if not o.dryrun:
  os.rmdir(dummydir)
