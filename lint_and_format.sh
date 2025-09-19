#!/bin/bash

target=$1

dirs="client dispatcher middleware resultnode results selectnode server"

if [ $# -ne 1 ] || ! [[ "$dirs" =~ (^|[[:space:]])"$target"($|[[:space:]]) ]] && [ "$target" != "all" ]; then
    echo "Uso: $0 <all | client | dispatcher | middleware | resultnode | results | selectnode | server>"
    exit 1
fi

if [ "$target" == "all" ]; then
    for folder in $dirs; do
        echo "Lint & Format: $folder"
        isort "$folder"
        black "$folder"
        flake8 --ignore=E501 "$folder"
        mypy "$folder" --disallow-untyped-defs
    done
else
    echo "Lint & Format: $target"
    isort "$target"
    black "$target"
    flake8 --ignore=E501 "$target"
    mypy "$target" --disallow-untyped-defs
fi
