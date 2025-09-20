#!/bin/bash

  printf 'DEFINE subscription FIELDS { "id": "int", "plan": "string" }'  | nc -w 10 127.0.0.1 7171
  printf 'STORE subscription FOR ctx1 PAYLOAD {"id": 1, "plan": "free"}' | nc -w 10 127.0.0.1 7171
  printf 'STORE subscription FOR ctx2 PAYLOAD {"id": 2, "plan": "pro"}' | nc -w 10 127.0.0.1 7171
  printf 'STORE subscription FOR ctx3 PAYLOAD {"id": 3, "plan": "premium"}' | nc -w 10 127.0.0.1 7171
  printf 'STORE subscription FOR ctx4 PAYLOAD {"id": 4, "plan": "enterprise"}' | nc -w 10 127.0.0.1 7171
  printf 'FLUSH' | nc -w 10 127.0.0.1 7171
