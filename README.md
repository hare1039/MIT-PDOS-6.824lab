# MIT-PDOS-6.824lab
This is my implementation of MIT PDOS 6.824 Lab

# Requirments
- go 1.9
- DO NOT USE `go get` or put in `GOPATH`. Please point GOPATH to the top level of the directory.

# Current Status
- All test in Raft and KVRaft passed (Including Racing)
- Sometimes will run into race condition due to the RPC call (About 1/10 times). I can't fix that because the architecture are provided that way.
