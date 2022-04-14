#!/bin/bash

source config.inc

mkdir -p cache

curl -s https://defacto-observatoire.fr/XWiki/DeFacto/FactCheck/Export?key=$APIKEY > cache/defacto-factchecks.json

function HTTPCODE {
  cat | head -1 | sed -r 's|^.*HTTP/[0-9.]+ ([0-9]+).*$|\1|'
}

echo "ID,URL,FIXEDURL,COMMENT"
python -m json.tool cache/defacto-factchecks.json   |
 grep '"original-url":\|^            "id":'         |
 awk -F '"' '{ print $4 }'                          |
 tr "\n" " "                                        |
 sed 's|Medias/|\n\rMedias/|g'                      |
 while read line; do
  id=$(echo $line | awk '{ print $1 }')
  url=$(echo $line | awk '{ print $2 }')
  goodurl=""
  comment=""
  if [ -z "$id" ]; then
    continue
  elif [ -z "$url" ]; then
    echo "$id,,,Empty URL"
    continue
  fi
  res=$(curl -sIL "$url")
  code=$(echo "$res" | HTTPCODE)
  if [ "$code" -ne 200 ]; then
    if echo "$code" | grep "^30" > /dev/null; then
      goodurl=$(echo "$res" | grep -i "Location:" | head -1 | sed 's/^.*:\s*//' | sed 's|^//|https://|' | sed 's/\r//')
      comment="FIXED redirection"
    elif [ "$code" -eq "404" ] && echo $url | grep "factuel.afp.com" > /dev/null; then
      inc=-1
      while [ -z "$goodurl" ] && [ "$inc" -lt 100 ]; do
        inc=$(($inc + 1))
        if curl -sIL "$url-$inc" | HTTPCODE | grep 200 > /dev/null; then
          goodurl="$url-$inc"
          comment="FIXED Factuel slug"
        fi
      done
    fi
    if [ -z "$goodurl" ]; then
      echo "$id,$url,,URL unavailable: $code"
    else
      echo "$id,$url,$goodurl,$comment"
    fi
  fi
 done
