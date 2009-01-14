#!/usr/bin/python

import glob
import Time as T
import System as S

'''
Backup my Neo.
'''

basedir = '/home/isilanes/Documents/FLOSS/Neo_FreeRunner/backup'
backdir = '%s_%i' % (T.day(),T.now())
remodir = 'root@neo:~/sync'
rsync   = 'rsync -rltouvh -L --progress'

# Find latest copy, to link to it wherever possible (don't do a full backup):
bdirs = glob.glob(basedir+'/*')
if bdirs:
  rsync += ' --link-dest=%s/ ' % (bdirs[-1])

cmnd = '%s %s/ %s/%s/' % (rsync,remodir,basedir,backdir)
print cmnd
S.cli(cmnd)
