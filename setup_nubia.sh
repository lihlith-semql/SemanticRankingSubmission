#!/bin/bash
git clone https://github.com/wl-research/nubia.git
cd nubia/
git reset --hard abf15bcb5a3c4a23c192fd53a48bab6b0a1e0cb3
pip install -r requirements.txt
cd ../
ln -s nubia/nubia_score/ .
