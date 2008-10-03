#!/usr/bin/python
# coding=utf-8

'''
myback
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

It makes incremental backups with rsync. It is a Python port of
backup_with_rsync.pl, by me.

USAGE

% myback.py $what $mach $often

where:

$what  = whether to back up a remote machine here ('from') or this machine
         to a remote one ('to').
$mach  = machine to get/put backup data (depending on $what)
$often = string with periodicity (daily, weekly and monthly are defined)

I always use this script with cron.

VERSION

svn_revision = 1

'''

import optparse
import os
import sys
import datetime

sys.path.append(os.environ['HOME']+'/WCs/PythonModules')

import System as S
import FileManipulation as FM

#--------------------------------------------------------------------------------#

# Read arguments:
parser = optparse.OptionParser()

parser.add_option("-o", "--origin",
                  dest="origin",
                  help="Originating machine. Default: localhost.",
                  default='localhost')

parser.add_option("-d", "--destination",
                  dest="destination",
                  help="Destination machine. Default: None.",
                  default=False)

parser.add_option("-v", "--verbose",
                  dest="verbose",
                  help="Whether to be verbose. Default: don\'t be.",
		  action="store_true",
                  default=False)

parser.add_option("-y", "--dryrun",
                  help="Dry run: do nothing, just tell what would be done. Default: real run.",
		  action="store_true",
                  default=False)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

class machine:

  def __init__(self,ip=None,user=None,dir=None,name=None):
    self.ip   = ip
    self.user = user
    self.dir  = dir
    self.name = name

  def remotedir(self,offset=0):
    '''
    Returns string with remote location for "offset" days from today.
      offset = offset from today, in days. 0 = today, -1 = yesterday...
    '''

    return '%s_%s' % (m.dir, gimme_date(offset))

  def checkdir(self,offset=0):
    '''
    Checks if remote backup dir already exists, and abort if it does.
      offset = offset from today, in days. 0 = today, -1 = yesterday...
    '''

    cmnd = 'ssh %s@%s "file %s && echo OK"' % (self.user, self.ip, self.remotedir(offset))

    if o.verbose:
      print cmnd

    exists = False
    if not o.dryrun:
      response = S.cli(cmnd,True)
      if 'OK' in '.'.join(response):
        exists = True

    if exists:
      sys.exit('Aborting: remote dir "%s" exists!' % (m.remotedir(0)))

  def backup(self,offset=0,rsync=None):
    '''
    '''

    cmnd = '%s %s' % (rsync, self.remotedir(0))

    doit(cmnd)

#--------------------------------------------------------------------------------#

def doit(cmnd=None):
  '''
  Print and/or execute command, depending on o.verbose and o.dryrun.
    cmnd = command to run
  '''

  if o.verbose:
    print cmnd

  if not o.dryrun:
    S.cli(cmnd)

#--------------------------------------------------------------------------------#

def read_config(config=None):
  
  lines = FM.file2array(config,'cb')

  props = {}
  for line in lines:
    aline = line.split()
    m = aline[0]
    props[m] = machine()
    props[m].name = m
    props[m].ip   = aline[1]
    props[m].user = aline[2]
    props[m].dir  = aline[3]

  return props

#--------------------------------------------------------------------------------#

def make_checks(machines):

  if o.origin == 'localhost':
    if o.destination:
      if not o.destination in machines:
        sys.exit('Error: destination "%s" is not available.' % (o.destination))

    else:
      sys.exit('Error: no destination machine entered.')

  else:
    o.destination = 'localhost'

#--------------------------------------------------------------------------------#

def gimme_date(offset=0):
  '''
  Gives a date with a given offset in days, with respect to today.
    offset = if 0, then it's today. If 1, it is tomorrow. If -2 is two days ago.
             You get the picture.
  '''
  
  day   = datetime.date.today()
  delta = datetime.timedelta(days=offset)
  day   = day + delta
  date  = day.strftime('%Y.%m.%d')

  return date

#--------------------------------------------------------------------------------#

if __name__ == '__main__':

  # General variables:
  rsync    = 'rsync -a -e ssh --delete --delete-excluded ' # base rsync command to use
  user     = os.environ['LOGNAME']                         # username of script user
  home     = os.environ['HOME']                            # your home dir
  logfile  = '%s/.LOGs/backup_log' % (home)                # file to put a log entry of what we did
  conf     = '%s/.LOGs/myback/config' % (home)             # configuration file
  cp       = '/bin/cp -P'                                  # command to use for copying files

  # Read configuration:
  machines = read_config(conf)

  # Make checks:
  make_checks(machines)

  # Bring up machine object:
  m = machines[o.destination]

  # Check if remote dir with same name present:
  m.checkdir(0)

  # Make backup:
  m.backup(0,rsync)

  # Then rsync:
