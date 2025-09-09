#!/bin/bash

SOCKET="/tmp/sneldb.sock"

socat - UNIX-CONNECT:$SOCKET < <(

  # DEFINE commands
  echo 'DEFINE subscription FIELDS { "id": "int", "plan": "string" }'
  echo 'DEFINE user_signup FIELDS { "user_id": "int", "email": "string", "country": "string" }'
  echo 'DEFINE payment FIELDS { "payment_id": "int", "user_id": "int", "amount": "int" }'
  echo 'DEFINE feature_usage FIELDS { "user_id": "int", "feature": "string", "duration": "int" }'

  # Plans and countries for indexing
  plans=("free" "pro" "business" "enterprise")
  countries=("NL" "DE" "FR" "ES" "US")
  features=("search" "dashboard" "export" "settings" "billing" "login")

  # STORE subscription (250)
  for i in $(seq 1 250); do
    plan=${plans[$((i % 4))]}
    echo "STORE subscription FOR ctx$((i % 10 + 1)) PAYLOAD {\"id\": $i, \"plan\": \"$plan\"}"
  done

  # STORE user_signup (250)
  for i in $(seq 1 250); do
    email="user$i@example.com"
    country=${countries[$((i % 5))]}
    echo "STORE user_signup FOR ctx$((i % 15 + 1)) PAYLOAD {\"user_id\": $i, \"email\": \"$email\", \"country\": \"$country\"}"
  done

  # STORE payment (248)
  for i in $(seq 1 248); do
    amount=$((100 + (i % 500)))  # 100–599
    echo "STORE payment FOR ctx$((i % 8 + 1)) PAYLOAD {\"payment_id\": $i, \"user_id\": $((i % 200 + 1)), \"amount\": $amount}"
  done

  # STORE feature_usage (248)
  for i in $(seq 1 248); do
    duration=$((i % 3600))
    feature=${features[$((i % 6))]}
    echo "STORE feature_usage FOR ctx$((i % 12 + 1)) PAYLOAD {\"user_id\": $((i % 300 + 1)), \"feature\": \"$feature\", \"duration\": $duration}"
  done

)