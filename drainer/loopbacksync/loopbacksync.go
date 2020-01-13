// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package loopbacksync

const (
	//MarkTableName mark table name
	MarkTableName = "retl._drainer_repl_mark"
	//ChannelID channel id
	ChannelID = "channel_id"
	//Val val
	Val = "val"
	//ChannelInfo channel info
	ChannelInfo = "stream_info"
)

//LoopBackSync loopback sync info
type LoopBackSync struct {
	ChannelID       int64
	LoopbackControl bool
	SyncDDL         bool
}

//NewLoopBackSyncInfo return LoopBackSyncInfo objec
func NewLoopBackSyncInfo(ChannelID int64, LoopbackControl, SyncDDL bool) *LoopBackSync {
	l := &LoopBackSync{
		ChannelID:       ChannelID,
		LoopbackControl: LoopbackControl,
		SyncDDL:         SyncDDL,
	}
	return l
}
