#!/bin/bash
cd /home/291928k/dev/projects/wa-health-ed-pipeline
git add -u
git commit -m "Rewrite notebook 01: remove separator dash lines, clean cells"
python3 scripts/sync_to_fabric.py 01
git add -u
git commit -m "Update notebook 01 Fabric ID after sync"
git checkout main
git merge dev --no-edit
git checkout dev
echo "Done."
