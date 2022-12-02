package bitswap_message_pb

import (
	_ "github.com/gogo/protobuf/gogoproto"
	libipfs "github.com/ipfs/go-libipfs/bitswap/message/pb"
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb. instead
type Message_BlockPresenceType = libipfs.Message_BlockPresenceType

const (
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Have instead
	Message_Have Message_BlockPresenceType = libipfs.Message_Have
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_DontHave instead
	Message_DontHave Message_BlockPresenceType = libipfs.Message_DontHave
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_BlockPresenceType_name instead
var Message_BlockPresenceType_name = libipfs.Message_BlockPresenceType_name

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_BlockPresenceType_value instead
var Message_BlockPresenceType_value = libipfs.Message_BlockPresenceType_value

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_WantType instead
type Message_Wantlist_WantType = libipfs.Message_Wantlist_WantType

const (
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_Block instead
	Message_Wantlist_Block Message_Wantlist_WantType = libipfs.Message_Wantlist_Block
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_Have instead
	Message_Wantlist_Have Message_Wantlist_WantType = libipfs.Message_Wantlist_Have
)

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_WantType_name instead
var Message_Wantlist_WantType_name = libipfs.Message_Wantlist_WantType_name

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_WantType_value instead
var Message_Wantlist_WantType_value = libipfs.Message_Wantlist_WantType_value

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message instead
type Message = libipfs.Message

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist instead
type Message_Wantlist = libipfs.Message_Wantlist

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Wantlist_Entry instead
type Message_Wantlist_Entry = libipfs.Message_Wantlist_Entry

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_Block instead
type Message_Block = libipfs.Message_Block

// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.Message_BlockPresence instead
type Message_BlockPresence = libipfs.Message_BlockPresence

var (
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.ErrInvalidLengthMessage instead
	ErrInvalidLengthMessage = libipfs.ErrInvalidLengthMessage
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.ErrIntOverflowMessage instead
	ErrIntOverflowMessage = libipfs.ErrIntOverflowMessage
	// Deprecated: use github.com/ipfs/go-libipfs/bitswap/message/pb.ErrUnexpectedEndOfGroupMessage instead
	ErrUnexpectedEndOfGroupMessage = libipfs.ErrUnexpectedEndOfGroupMessage
)
