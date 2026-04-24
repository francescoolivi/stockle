#!/bin/bash
cd ~/Documents/Claude/Projects/Stockle
git add -A
git commit -m "update: $(date '+%Y-%m-%d %H:%M')"
git push
echo "✓ Stockle aggiornato su GitHub Pages"
