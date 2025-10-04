#!/usr/bin/env bash
# dim_file_update.sh v1.0.6 - Saturday, October 4, 2025
_me="${0##*/}"

set -eu

HOME_DIRS=('Pictures' 'Videos' '.cache/LevellTech/Recent' 'Downloads' 'Torrents')

cd ~/Projects/FileDimension

for D in ${HOME_DIRS[@]}; do
	pdm run python -m src.file_dimension.cli scan --max-files=-1 --no-prune \
		~/$D >$TMP/$_me.log 2>$TMP/$_me.err
done
pdm run python -m src.file_dimension.cli find-dupes
