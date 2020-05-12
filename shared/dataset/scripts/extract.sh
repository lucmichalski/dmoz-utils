#!/bin/sh

find . -maxdepth 1 -type f -name "*.gz" | xargs -I {} sh -c "7z x {}"
