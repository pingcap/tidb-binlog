#!/bin/bash

set -e
# test passed in pandoc 1.19.1

MAINFONT="Hiragino Sans GB W3"
MONOFONT="Hiragino Sans GB W3"

# MAINFONT="Tsentsiu Sans HG"
# MONOFONT="Tsentsiu Sans Console HG"

#_version_tag="$(date '+%Y%m%d').$(git rev-parse --short HEAD)"
_version_tag="$(date '+%Y%m%d')"

cd docs && pandoc -N --toc --smart --latex-engine=xelatex \
    --template=templates/template.tex \
    --listings \
    -V title="TiDB-Binlog 中文文档" \
    -V author="PingCAP Inc." \
    -V date="v1.0.0\$\sim\$rc2+${_version_tag}" \
    -V CJKmainfont="${MAINFONT}" \
    -V mainfont="${MAINFONT}" \
    -V sansfont="${MAINFONT}" \
    -V monofont="${MONOFONT}" \
    -V geometry:margin=1in \
    -V include-after="\\input{templates/copyright.tex}" \
    doc-cn.md -o tidb-binlog-document.pdf
