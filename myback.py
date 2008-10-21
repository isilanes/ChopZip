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

import datetime
import glob
import optparse
import os
import re
import sys

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

  machines = []
  for machine in [o.source,o.destination]:
    conf_file = '%s/%s.conf' % (conf,machine)
    
    lines = FM.file2array(conf_file,'cb')

    props = {}
    for line in lines:
      aline = line.replace('\n','').split('=')
      props[aline[0]] = aline[1]

    machines.append(props)

  return machines

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

def gimme_date(offset=0,seconds=None):
  '''
  Gives a date with a given offset in days, with respect to today.
    offset = if 0, then it's today. If 1, it is tomorrow. If -2 is two days ago.
             You get the picture.
  '''
  
  if seconds:
    sec   = datetime.datetime.today()
    date  = sec.strftime('%Y.%m.%d %H:%M:%S')

  else:
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
 
  gd0 = gimme_date(0)
  mm  = machines[1]
  mms = mm['SSHCOMM']
  mmt = mm['TODIR']

  cmnd = '%s "file %s_%s && echo OK"' % (mms, mmt, gd0)
  test = S.cli(cmnd,True)

  if test and test[-1] != 'OK\n':
    for i in range(1,maxt+1):
      gdi = gimme_date(-i)
      cmnd = '%s "file %s_%s && echo OK"' % (mms, mmt, gdi)
      test = S.cli(cmnd,True)
      if test[-1] == 'OK\n':
        cmnd = '%s "cp -al %s_%s %s_%s"' % (mms, mmt, gdi, mmt, gd0)
        doit(cmnd)
	break

#--------------------------------------------------------------------------------#

def write_log(file):
  '''
  Save log entry.
  '''

  logstring = 'Backed up FROM: %s TO: %s AT: %s\n' % (o.source, o.destination, gimme_date(0,True))

  FM.w2file(logfile,logstring,'a')

#--------------------------------------------------------------------------------#

def build_rsync(in_rsync):
  '''
  Build a more complete rsync command.
  '''

  # Global excludes:
  out_rsync = '%s --exclude-from=%s/global.excludes ' % (in_rsync, conf)

  # Verbosity:
  if o.verbose:
    out_rsync += ' -vh --progress '

  return out_rsync

#--------------------------------------------------------------------------------#

def find_deletable(m):
  '''
  Find old, deletable, backups.
  '''

  mm = m[1]
  mmt = mm['TODIR']

  cmnd = '%s "/bin/ls -d %s*"' % (mm['SSHCOMM'],mmt)
  dirlist = S.cli(cmnd,True)

  rejects_by_name = []

  dates = []
  for dn in dirlist:
    dn  = dn.replace('\n','')
    d2d = dir2date(dn)

    if d2d == None:
      rejects_by_name.append(dn)

    else:
      dates.append(d2d)

  maxd   = 0
  valids = {0:None}
  for v in mm['VALIDBACKS'].split():
    ii,jj = [int(x) for x in v.split(':')]
    for i in range(ii):
      ij = (i+1)*jj
      valids[ij] = None
      if ij > maxd:
        maxd = ij

  rejects = []
  for d in dates:
    accepted = False
    for di in range(d,maxd+1):
      if valids.has_key(di):
        if not valids[di]:
	  valids[di] = d
          accepted = True
	  break

    if not accepted:
      rejects.append(d)

  print "The following dirs should be deleted:"

  for dn in rejects_by_name:
    print dn

  for r in rejects:
    print gimme_dir(r,mm)

  print "\nThe following dirs should be kept:"

  for v in valids:
    if not valids[v] == None:
      print "DIR:  %s  IN BEHALF OF:  %s" % (gimme_dir(-valids[v],mm), gimme_dir(-v,mm))

#--------------------------------------------------------------------------------#

def gimme_dir(i=0,m=None):
  '''
  Given a day offset and machine conf, return backup dir.
  '''

  return "%s_%s" % (m['TODIR'],gimme_date(i))

#--------------------------------------------------------------------------------#

def dir2date(dirname=None):
  '''
  Returns the day offset of a given dir.
  '''

  offset = dirname.split('_')[-1]
  if re.match('....\...\...',offset):
    y,m,d  = [int(x) for x in offset.split('.')]
    delta  = datetime.date.today() - datetime.date(y,m,d)
    offset = delta.days

  else:
    offset = None

  return offset

#--------------------------------------------------------------------------------#

if __name__ == '__main__':

  # General variables:
  rsync    = 'rsync -a -e ssh --delete --delete-excluded ' # base rsync command to use
  user     = os.environ['LOGNAME']                         # username of script user
  home     = os.environ['HOME']                            # your home dir
  conf     = '%s/.myback' % (home)                         # configuration dir
  logfile  = '%s/myback.log' % (conf)                      # log file
  mxback   = 10                                            # max number of days to go back searching for latest dir

  # Build rsync command:
  rsync = build_rsync(rsync)

  # Hook to SSH agent:
  P.ssh_hook(user)

  # Make checks:
  make_checks(o)

  # Read configurations:
  m = read_config(o)

  # Copy last available (whithin specified limit) to "current":
  cp_last(m,mxback)

  # Determine if any to delete:
  find_deletable(m)

  # Make backup:
  backup(m,0)

  # At last, log:
  write_log(logfile)
