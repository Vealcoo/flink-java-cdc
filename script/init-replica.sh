#!/bin/bash

mongosh --username admin --password admin --authenticationDatabase admin <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongodb_primary:27017", priority: 2 },
    { _id: 1, host: "mongodb_secondary:27017", priority: 1 },
    { _id: 2, host: "mongodb_arbiter:27017", arbiterOnly: true }
  ]
})
EOF
