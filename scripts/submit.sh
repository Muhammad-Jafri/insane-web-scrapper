#!/bin/bash
set -e

URLS_FILE="${1:-urls.txt}"
API_URL="${2:-http://localhost:8000}"

if [ ! -f "$URLS_FILE" ]; then
  echo "Usage: $0 [urls_file] [api_url]"
  echo "  urls_file defaults to urls.txt"
  echo "  api_url   defaults to http://localhost:8000"
  exit 1
fi

# Convert:
# url_1
# url_2
# url_3
#
# into:
# "url_1","url_2","url_3"

URLS=$(awk 'NF {printf "\"%s\",", $0}' "$URLS_FILE" | sed 's/,$//')

curl -X POST "$API_URL/jobs/bulk" \
  -H "Content-Type: application/json" \
  -d "{\"urls\": [$URLS]}"