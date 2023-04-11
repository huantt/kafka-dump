package model

type NftBalance struct {
	Id           int64  `json:"-"`
	Chain        string `json:"chain"`
	Address      string `json:"address"`
	TokenAddress string `json:"token_address"`
	TokenId      string `json:"token_id"`
	Amount       string `json:"amount"`
	LatestBlock  int64  `json:"latest_block"`
	EventType    string `json:"event_type"`
}
