#!/usr/bin/python
# coding=utf-8

'''
increback
(c) 2008-2009, Iñaki Silanes

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
backup_with_rsync.pl (also by me).

USAGE

For usage, run:

% increback.py -h

I use this script interactively.
'''

import datetime
import glob
import optparse
import os
import re
import sys
import operator

sys.path.append(os.environ['HOME']+'/pythonlibs')

import Private as P
import System as S
import FileManipulation as FM
import Time as T

#--------------------------------------------------------------------------------#

# Read arguments:
parser = optparse.OptionParser()

parser.add_option("-c", "--config",
                  help="Configuration file. Default: None.",
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

# Make dry runs more verbose:
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

def read_config(options=None):

  conf_file = '{0}/{1}.conf'.format(conf_dir,options.config)
    
  lines = FM.file2array(conf_file,'cb')

  props = {}
  for line in lines:
    aline = line.replace('\n','').split('=')
    props[aline[0]] = '='.join(aline[1:])

  return props

#--------------------------------------------------------------------------------#

def make_checks(o):

  pass

#--------------------------------------------------------------------------------#

def backup(config=None,rsync=None,last_dir=None,offset=0):
  '''
  Actually make the backup.
  '''

  success = False

  if config:

    if last_dir:
      rsync = '{0} --link-dest={1}'.format(rsync, last_dir)

    # Actually do it:
    cmnd = '{0} {1[FROMDIR]}/ {1[TODIR]}/{2}/'.format(rsync,config,T.gimme_date(offset))
    doit(cmnd)

    success = True

  else:
    print "Could not back up: no source/destination machine(s) specified!"

  return success

#--------------------------------------------------------------------------------#

def find_last_dir(config=None,maxd=1, verbosity=0):
  '''
  Find last available dir into which rsync will hardlink unmodified files.
    maxd = max number of days we want to move back.
  '''

  gd0 = T.gimme_date(0)
  mmt = config['TODIR']

  for i in range(1,maxd+1):
    gdi = T.gimme_date(-i)
    dir = '{0}/{1}'.format(mmt,gdi)
    if verbosity > 1:
      print dir
    if os.path.isdir(dir):
      return dir

  return None

#--------------------------------------------------------------------------------#

def write_log(file):
  '''
  Save log entry.
  '''

  logstring = 'Backed up FROM: %s TO: %s AT: %s\n' % (o.source, o.destination, T.gimme_date(0,True))

  FM.w2file(logfile,logstring,'a')

#--------------------------------------------------------------------------------#

def build_rsync(in_rsync,config=None):
  '''
  Build a more complete rsync command.
  '''

  # Global excludes:
  out_rsync = '{0} --exclude-from={1}/global.excludes '.format(in_rsync, conf_dir)

  # Verbosity:
  if o.verbosity > 0:
    out_rsync += ' -vh '
    out_rsync += ' --progress '

  # Machine-specific options:
  try:
    out_rsync += ' {0} '.format(config['RSYNCOPS'])

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
  dirlist = S.cli(cmnd,1).split('\n')

  dirlist = []
  for element in S.cli(cmnd,1).split('\n'):
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

  return "%s_%s" % (m['TODIR'],T.gimme_date(i))

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
  rsync    = 'rsync -rltou --delete --delete-excluded ' # base rsync command to use
  user     = os.environ['LOGNAME']                      # username of script user
  home     = os.environ['HOME']                         # your home dir
  conf_dir = '%s/.increback' % (home)                   # configuration dir
  logfile  = '%s/.LOGs/increback.log' % (home)          # log file
  mxback   = 60                                         # max number of days to go back 
                                                        # searching for latest dir

  # Read configurations:
  if o.verbosity > 0:
    print "Reading config files...",

  cfg = read_config(o)

  if o.verbosity > 0: print " OK"

  # Build rsync command:
  if o.verbosity > 0: print "Building rsync command...",

  rsync = build_rsync(rsync,cfg)

  if o.verbosity > 0: print " OK"

  # Make checks:
  if o.verbosity > 0: print "Performing various checks...",

  make_checks(o)

  if o.verbosity > 0: print " OK"

  # Find last available dir (whithin specified limit) to hardlink to when unaltered:
  if o.verbosity > 0: print "Determining last 'linkable' dir...",

  last_dir = find_last_dir(cfg,mxback,o.verbosity)

  if o.verbosity > 0: print " -> '%s'" % (last_dir)

  # Determine if any to delete:
  #if o.verbosity > 1:
  #  print "Finding out deletable dirs..."
  #
  #find_deletable(cfg)

  # Make backup:
  if o.verbosity > 0:
    print "Doing actual backup..."

  success = backup(cfg,rsync,last_dir,0)

  '''
  # At last, log:
  if not o.dryrun and success:

    if o.verbosity > 0: print "Logging info and exiting."

    write_log(logfile)
  '''
