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

For usage, run:

% myback.py -h

I always use this script with cron.

VERSION

svn_revision = r20 (2008-11-18 12:02:44)

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

parser.add_option("-s", "--source",
                  dest="source",
                  help="Source machine. Default: localhost.",
                  default='localhost')

parser.add_option("-d", "--destination",
                  dest="destination",
                  help="Destination machine. Default: None.",
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

parser.add_option("--suspend",
                  help="Request that the system be suspended after performing the backup. This is achieved by placing a file where a root cron job will read it and suspend if found. Default: Do not suspend.",
		  action="store_true",
                  default=False)

parser.add_option("--hibernate",
                  help="Request that the system be hibernated after performing the backup. This is achieved by placing a file where a root cron job will read it and hibernate if found. Default: Do not hibernate.",
		  action="store_true",
                  default=False)

(o,args) = parser.parse_args()

#--------------------------------------------------------------------------------#

# Make dry runs verbose:
if o.dryrun:
  o.verbosity = 2

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

def read_config(options=None):

  machines = []
  for machine in [o.source,o.destination]:
    conf_file = '%s/%s.conf' % (conf,machine)
    
    lines = FM.file2array(conf_file,'cb')

    props = {}
    for line in lines:
      aline = line.replace('\n','').split('=')
      props[aline[0]] = '='.join(aline[1:])

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

def backup(machines=None,rsync=None,last_dir=None,offset=0):
  '''
  Actually make the backup.
  '''

  success = False

  if machines:
    src = machines[0]
    dst = machines[1]

    if last_dir:
      rsync = '%s --link-dest=%s' % (rsync, last_dir)

    # Actually do it:
    cmnd = '%s %s/ %s:%s_%s/' % (rsync, src['FROMDIR'], dst['RSYNCAT'], dst['TODIR'],gimme_date(offset))
    doit(cmnd)

    success = True

  else:
    print "Could not back up: no source/destination machine(s) specified!"

  return success

#--------------------------------------------------------------------------------#

def find_last_dir(machines=None,maxt=1):
  '''
  Find last available dir into which rsync will hardlink unmodified files.
    maxd = max number of days we want to move back.
  '''
 
  gd0 = gimme_date(0)
  mm  = machines[1]
  mmt = mm['TODIR']
  mat = mm['RSYNCAT']

  link_dir = None

  for i in range(1,maxt+1):
    gdi = gimme_date(-i)
    cmnd = 'echo "ls %s_%s" | sftp -b - %s 2> /dev/null && echo OK' % (mmt, gdi, mat)
    test = S.cli(cmnd,True)
    if test and test[-1] == 'OK\n':
      link_dir = '%s_%s' % (mmt, gdi)
      break

  return link_dir

#--------------------------------------------------------------------------------#

def write_log(file):
  '''
  Save log entry.
  '''

  logstring = 'Backed up FROM: %s TO: %s AT: %s\n' % (o.source, o.destination, gimme_date(0,True))

  FM.w2file(logfile,logstring,'a')

#--------------------------------------------------------------------------------#

def build_rsync(in_rsync,machines=None):
  '''
  Build a more complete rsync command.
  '''

  # Global excludes:
  out_rsync = '%s --exclude-from=%s/global.excludes ' % (in_rsync, conf)

  # Verbosity:
  if o.verbosity > 2:
    out_rsync += ' -vh '

    if o.verbosity > 3:
      out_rsync += ' --progress '

  # Machine-specific options:
  try:
    out_rsync += ' %s ' % (machines[1]['RSYNCOPS'])

  except:
    pass

  return out_rsync

#--------------------------------------------------------------------------------#

def find_deletable(m):
  '''
  Find old, deletable, backups.
  '''

  mm  = m[1]
  mmt = mm['TODIR']
  mat = mm['RSYNCAT']

  mmt_bare = mmt.split('/')[:-1]
  mmt_bare = '/'.join(mmt_bare)

  cmnd = 'echo "cd %s/\\nls" | sftp -b - %s | grep -v ">"' % (mmt_bare,mat)
  dirlist = S.cli(cmnd,True)

  dirlist = []
  for element in S.cli(cmnd,True):
    element = re.sub(' *\n','',element)
    elements = element.split()
    dirlist.extend(elements)

  rejects_by_name = []

  dates = []
  for dn in dirlist:
    dn  = dn.replace(' *\n','')
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
    for i in range(1,ii+1):
      ij = i*jj
      valids[ij] = None
      if ij > maxd:
        maxd = ij

  exists = {}
  rejects = []
  for d in dates:
    exists[d] = True
    accepted = False
    for di in range(d,maxd+1):
      if valids.has_key(di):
        if not valids[di]:
	  valids[di] = d
          accepted = True
	  break

    if not accepted:
      rejects.append(d)

  '''
  Table with summary of dirs that exist, dirs that should be backed up,
  and crossings of the two lists.
  '''

  print "Day  Exists?  Save? Choice"
  print "---  -------  ----- ------"

  for i in range(maxd+1):
    print "%3i " % (i),

    if exists.has_key(i ): print " exists ",
    else:                  print "   --   ",

    if valids.has_key(i):
      print " save " ,

      if valids[i] != None: print "  %3i " % (valids[i]),
      else:                 print "    - ",

    else: print "  --  ",
    
    print ''
  print ''

  # Suggest to delete:
  if rejects or rejects_by_name:
    print "The following dirs should be deleted:"
  
    if rejects_by_name:
      print "*) Not named by date:"
      for dn in rejects_by_name:
        print dn

    if rejects:
      print "*) Its date is not needed:"
      for r in rejects:
        print gimme_dir(-r,mm)

  # Suggest to keep:
  if valids:
    print "\nThe following dirs should be kept:"
    for v in sorted(valids.iteritems(), key=operator.itemgetter(1)):
      if not v[1] == None:
        if v[0] == v[1]:
          print "DIR:  %s  AS ITSELF" % (gimme_dir(-v[1],mm))
  
        else:
          print "DIR:  %s  IN BEHALF OF:  %s" % (gimme_dir(-v[1],mm), gimme_dir(-v[0],mm))

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
  rsync    = 'rsync -a --delete --delete-excluded ' # base rsync command to use
  user     = os.environ['LOGNAME']                  # username of script user
  home     = os.environ['HOME']                     # your home dir
  conf     = '%s/.myback' % (home)                  # configuration dir
  logfile  = '%s/.LOGs/myback.log' % (home)         # log file
  mxback   = 10                                     # max number of days to go back searching for latest dir

  # Read configurations:
  if o.verbosity > 0:
    print "Reading config files...",

  m = read_config(o)

  if o.verbosity > 0: print " OK"

  # Build rsync command:
  if o.verbosity > 0: print "Building rsync command...",

  rsync = build_rsync(rsync,m)

  if o.verbosity > 0: print " OK"

  # Hook to SSH agent:
  if o.verbosity > 0: print "Hooking to SSH agent...",

  P.ssh_hook(user)

  if o.verbosity > 0: print " OK"

  # Make checks:
  if o.verbosity > 0: print "Performing various checks...",

  make_checks(o)

  if o.verbosity > 0: print " OK"

  # Find last available dir (whithin specified limit) to hardlink to when unaltered:
  if o.verbosity > 0: print "Determining last 'linkable' dir...",

  last_dir = find_last_dir(m,mxback)

  if o.verbosity > 0: print " -> '%s'" % (last_dir)

  # Determine if any to delete:
  if o.verbosity > 1:
    print "Finding out deletable dirs..."
    find_deletable(m)

  # Make backup:
  if o.verbosity > 0:
    print "Doing actual backup..."
  success = backup(m,rsync,last_dir,0)

  # At last, log:
  if not o.dryrun and success:

    if o.verbosity > 0: print "Logging info and exiting."

    write_log(logfile)

    # If requested, place a file in a special location (~/.LOGs/). A root cron job should be running
    # (in principle /root/bin/shutdown_if_requested.py), that will suspend/hibernate the PC if it
    # finds the aforementioned file.
    if o.suspend:
      suspend_file = '%s/.LOGs/please_suspend_me' % (home)
      FM.w2file(suspend_file,'myback.py asks for suspend...\n')

    if o.hibernate:
      hibernate_file = '%s/.LOGs/please_hibernate_me' % (home)
      FM.w2file(hibernate_file,'myback.py asks for hibernate...\n')
