#!/bin/bash

# Variables
REST=$1
DUPLICITY=/usr/bin/duplicity
DEST=ssh://b395676@backup.dreamhost.com//home/b395676/duplicity/flanders.home/
VERBOSITY=5
export TMPDIR=$HOME/.myback/tmp

# PW for encryption:
export PASSPHRASE='e/%$13oqr31i5r652adlhqgb'

# PW for SSH:
export SSH_AUTH_SOCK=`find /tmp/keyring-* -name 'socket.ssh' -user isilanes`

# Ask to not suspend:
touch /home/isilanes/.LOGs/keep_me_up.dupliback

# Execute
CMND="$DUPLICITY --file-to-restore $REST -v$VERBOSITY $DEST ./"
echo $CMND
#$CMND

# Let it suspend:
rm -f /home/isilanes/.LOGs/keep_me_up.dupliback

# Even ASK for it:
#touch /home/isilanes/.LOGs/please_suspend_me
