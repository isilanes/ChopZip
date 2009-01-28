#!/bin/bash

# Variables
DUPLICITY=/usr/bin/duplicity
SOURCE=/home/isilanes/Downloads/kk/
DEST=ssh://b395676@backup.dreamhost.com//home/b395676/duplicity/flanders.home/
OPTS="--full-if-older-than 1M --volsize 25 -v0 --exclude-filelist /home/isilanes/.myback/global.excludes"
#OPTS="$OPTS --no-print-statistics"

# PW for encryption:
export PASSPHRASE='e/%$13oqr31i5r652adlhqgb'

# PW for SSH:
export SSH_AUTH_SOCK=`find /tmp/ssh* -name 'agent.*' -user isilanes`

# Ask not to suspend:
touch /home/isilanes/.LOGs/keep_me_up.dupliback

# Execute
echo $DUPLICITY $OPTS $SOURCE $DEST
$DUPLICITY $OPTS $SOURCE $DEST

# Let it suspend:
rm -f /home/isilanes/.LOGs/keep_me_up.dupliback

# Even ASK for it:
touch /home/isilanes/.LOGs/please_suspend_me
