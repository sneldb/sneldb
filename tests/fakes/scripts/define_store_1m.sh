#!/bin/bash

SOCKET="/tmp/sneldb.sock"

# Define 10 truly distinct context IDs
contexts=(
  "A9f3z1"
  "PQR998dl"
  "ZZtop88"
  "42LIGHTYEARS"
  "h0bbycat777"
  "xENoN2312"
  "CLOUDPILLAR"
  "d4rkKnght"
  "W1LLOWTR33"
  "SunMoon555"
)

# Value sets
plans=("free" "pro" "business" "enterprise")
countries=("NL" "DE" "FR" "ES" "US")
features=("search" "dashboard" "export" "settings" "billing" "login")
statuses=("paid" "unpaid" "pending")
priorities=("low" "medium" "high")
devices=("ios" "android" "web")

records_per_type=100000
batch_size=5000

socat - UNIX-CONNECT:$SOCKET < <(

  # DEFINE schemas
  echo 'DEFINE subscription FIELDS { "id": "string", "plan": "string" }'
  echo 'DEFINE user_signup FIELDS { "user_id": "string", "email": "string", "country": "string" }'
  echo 'DEFINE payment FIELDS { "payment_id": "string", "user_id": "string", "amount": "string" }'
  echo 'DEFINE feature_usage FIELDS { "user_id": "string", "feature": "string", "duration": "string" }'
  echo 'DEFINE invoice FIELDS { "invoice_id": "string", "total": "string", "status": "string" }'
  echo 'DEFINE login FIELDS { "user_id": "string", "ip": "string", "device": "string" }'
  echo 'DEFINE logout FIELDS { "user_id": "string", "duration": "string" }'
  echo 'DEFINE email_open FIELDS { "user_id": "string", "campaign": "string" }'
  echo 'DEFINE file_upload FIELDS { "user_id": "string", "filename": "string", "size": "string" }'
  echo 'DEFINE support_ticket FIELDS { "ticket_id": "string", "user_id": "string", "priority": "string" }'

  for ((i = 0; i < records_per_type; i++)); do
    if (( i % batch_size == 0 )); then
      echo "Generating batch $i / $records_per_type ..." >&2
    fi

    ctx=${contexts[$((i % 10))]}
    id="$i"
    user_id="$((i % 50000))"
    duration="$((i % 3600))"
    total="$((500 + i % 1000))"
    size="$((1000 + i % 500))"

    echo "STORE subscription FOR $ctx PAYLOAD {\"id\": \"$id\", \"plan\": \"${plans[$((i % 4))]}\"}"
    echo "STORE user_signup FOR $ctx PAYLOAD {\"user_id\": \"$id\", \"email\": \"user$id@example.com\", \"country\": \"${countries[$((i % 5))]}\"}"
    echo "STORE payment FOR $ctx PAYLOAD {\"payment_id\": \"$id\", \"user_id\": \"$user_id\", \"amount\": \"100\"}"
    echo "STORE feature_usage FOR $ctx PAYLOAD {\"user_id\": \"$user_id\", \"feature\": \"${features[$((i % 6))]}\", \"duration\": \"$duration\"}"
    echo "STORE invoice FOR $ctx PAYLOAD {\"invoice_id\": \"$id\", \"total\": \"$total\", \"status\": \"${statuses[$((i % 3))]}\"}"
    echo "STORE login FOR $ctx PAYLOAD {\"user_id\": \"$user_id\", \"ip\": \"192.168\", \"device\": \"${devices[$((i % 3))]}\"}"
    echo "STORE logout FOR $ctx PAYLOAD {\"user_id\": \"$user_id\", \"duration\": \"200\"}"
    echo "STORE email_open FOR $ctx PAYLOAD {\"user_id\": \"$user_id\", \"campaign\": \"campaign_$((i % 20))\"}"
    echo "STORE file_upload FOR $ctx PAYLOAD {\"user_id\": \"$user_id\", \"filename\": \"file_$id.txt\", \"size\": \"$size\"}"
    echo "STORE support_ticket FOR $ctx PAYLOAD {\"ticket_id\": \"$id\", \"user_id\": \"$user_id\", \"priority\": \"${priorities[$((i % 3))]}\"}"
  done

)