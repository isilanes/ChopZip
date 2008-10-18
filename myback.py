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

% myback.py -h

I always use this script with cron.

VERSION

svn_revision = 1

'''

import optparse
import os
import sys
import datetime

sys.path.append(os.environ['HOME']+'/WCs/PythonModules')

import Private as P
import System as S
import FileManipulation as FM

#--------------------------------------------------------------------------------#

# Read arguments:
parser = optparse.OptionParser()

parser.add_option("-s", "--source",
                  dest="source",
                  help="Source machine. Default: localhost.",
                  default='localhost')

parser.add_option("-d", "--destination",
                  dest="destination",
                  help="Destination machine. Default: None.",
                  default=None)

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

# Make dry runs verbose:
if o.dryrun:
  o.verbose = True

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

def read_config(options=None):

  result = []
  for machine in [o.source,o.destination]:
    conf_file = '%s/%s.conf' % (conf,machine)
    
    lines = FM.file2array(conf_file,'cb')

    props = {}
    for line in lines:
      aline = line.replace('\n','').split('=')
      props[aline[0]] = aline[1]

    result.append(props)

  return result

#--------------------------------------------------------------------------------#

def make_checks(o):

  if o.source == 'localhost':
    if o.destination:
      conf_file = '%s/%s.conf' % (conf,o.destination)
      if not os.path.isfile(conf_file):
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

def backup(machines=None,offset=0):
  '''
  Actually make the backup.
  '''

  src = machines[0]
  dst = machines[1]

  cmnd = '%s %s/ %s%s_%s/' % (rsync, src['FROMDIR'], dst['RSYNCAT'], dst['TODIR'],gimme_date(offset))
  doit(cmnd)

#--------------------------------------------------------------------------------#

def cp_last(machines=None,maxt=1):
  '''
  Make a copy of last available dir into "current".
    maxd = max number of days we want to move back.
  '''
 
  mm = machines[1]

  mms = mm['SSHCOMM']
  mmt = mm['TODIR']
  gd0 = gimme_date(0)
  for i in range(1,maxt+1):
    gdi = gimme_date(-i)
    cmnd = '%s "file %s_%s && echo OK"' % (mms, mmt, gdi)
    test = S.cli(cmnd,True)
    if test[-1] == 'OK\n':
      cmnd = '%s "cp -al %s_%s %s_%s"' % (mms, mmt, gdi, mmt, gd0)
      doit(cmnd)

#--------------------------------------------------------------------------------#

if __name__ == '__main__':

  # General variables:
  rsync    = 'rsync -a -e ssh --delete --delete-excluded ' # base rsync command to use
  user     = os.environ['LOGNAME']                         # username of script user
  home     = os.environ['HOME']                            # your home dir
  logfile  = '%s/.LOGs/backup_log' % (home)                # file to put a log entry of what we did
  conf     = '%s/.myback' % (home)                         # configuration dir
  mxback   = 10                                            # max number of days to go back searching for latest dir

  # Hook to SSH agent:
  P.ssh_hook(user)

  # Make checks:
  make_checks(o)

  # Read configurations:
  m = read_config(o)

  # Copy last available (whithin specified limit) to "current":
  cp_last(m,mxback)

  # Make backup:
  backup(m,0)

  # At last, log:

