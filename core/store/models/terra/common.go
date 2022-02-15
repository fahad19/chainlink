package terra

import sdk "github.com/cosmos/cosmos-sdk/types"

// SendRequest represents a request to transfer Terra coins.
type SendRequest struct {
	DestinationAddress sdk.AccAddress `json:"address"`
	FromAddress        sdk.AccAddress `json:"from"`
	Coins              string         `json:"coins"`
	TerraChainID       string         `json:"terraChainID"`
	AllowHigherAmounts bool           `json:"allowHigherAmounts"`
}
