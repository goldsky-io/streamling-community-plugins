#!/usr/bin/env bash
# Writes ERC-7984 ConfidentialTransfer logs of one token as CSV in the shape the
# zama_decrypt examples read. Needs curl and jq.
#
# Usage: fetch-confidential-transfers.sh <rpc_url> <token> <from_block> <to_block> > transfers.csv
set -euo pipefail
rpc=$1 token=$2 from=$(printf '0x%x' "$3") to=$(printf '0x%x' "$4")
topic0=0x67500e8d0ed826d2194f514dd0d8124f35648ab6e3fb5e6ed867134cffe661e9

curl -sf "$rpc" -H 'content-type: application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getLogs\",
  \"params\":[{\"address\":\"$token\",\"topics\":[\"$topic0\"],\"fromBlock\":\"$from\",\"toBlock\":\"$to\"}]}" |
  jq -r '
    def dec: ltrimstr("0x") | explode | reduce .[] as $c (0; . * 16 + $c - (if $c >= 97 then 87 else 48 end));
    if .error then error(.error.message) else . end |
    "id,block_number,block_hash,transaction_hash,log_index,address,topic0,topic1,topic2,topic3,data",
    (.result[] | [
      .transactionHash + "_" + (.logIndex | dec | tostring), (.blockNumber | dec), .blockHash,
      .transactionHash, (.logIndex | dec), (.address | ascii_downcase),
      .topics[0], .topics[1], .topics[2], .topics[3], .data
    ] | join(","))'
