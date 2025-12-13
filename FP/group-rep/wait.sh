#!/usr/bin/env bash
for p in 3306 3307 3308; do
  echo "waiting mysql on port $p..."
  until mysql -h 127.0.0.1 -P $p -uroot -prootpassword -e "SELECT 1" >/dev/null 2>&1; do
    sleep 1
  done
done
