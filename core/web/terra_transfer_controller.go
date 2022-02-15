package web

import (
	"fmt"
	"net/http"

	sdk "github.com/cosmos/cosmos-sdk/types"
	bank "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"

	"github.com/smartcontractkit/chainlink-terra/pkg/terra/client"
	"github.com/smartcontractkit/chainlink/core/services/chainlink"
	terramodels "github.com/smartcontractkit/chainlink/core/store/models/terra"
	"github.com/smartcontractkit/chainlink/core/web/presenters"
)

// TerraTransfersController can send LINK tokens to another address
type TerraTransfersController struct {
	App chainlink.Application
}

// Create sends Luna from the Chainlink's account to a specified address.
func (tc *TerraTransfersController) Create(c *gin.Context) {
	var tr terramodels.SendRequest
	if err := c.ShouldBindJSON(&tr); err != nil {
		jsonAPIError(c, http.StatusBadRequest, err)
		return
	}
	if tr.TerraChainID == "" {
		jsonAPIError(c, http.StatusBadRequest, errors.New("missing terraChainID"))
		return
	}
	chain, err := tc.App.GetChains().Terra.Chain(tr.TerraChainID)
	switch err {
	case ErrInvalidChainID, ErrMultipleChains, ErrMissingChainID:
		jsonAPIError(c, http.StatusUnprocessableEntity, err)
		return
	case nil:
		break
	default:
		jsonAPIError(c, http.StatusInternalServerError, err)
		return
	}

	if tr.FromAddress.Empty() {
		jsonAPIError(c, http.StatusUnprocessableEntity, errors.Errorf("withdrawal source address is missing: %v", tr.FromAddress))
		return
	}

	var decCoins sdk.DecCoins
	decCoins, err = sdk.ParseDecCoins(tr.Coins)
	if err != nil {
		jsonAPIError(c, http.StatusBadRequest, fmt.Errorf("invalid coins: %v", err))
		return
	}
	coins := sdk.NormalizeCoins(decCoins)
	for _, coin := range coins {
		if !coin.Amount.IsPositive() {
			jsonAPIError(c, http.StatusBadRequest, fmt.Errorf("amount must be positive: %s", coin))
			return
		}
	}

	if !tr.AllowHigherAmounts {
		var reader client.Reader
		reader, err = chain.Reader("")
		if err != nil {
			jsonAPIError(c, http.StatusInternalServerError, errors.Errorf("chain unreachable: %v", err))
			return
		}
		//opt: query AllBalances instead
		for _, coin := range coins {
			err = ValidateTerraBalanceForTransfer(reader, tr.FromAddress, coin)
			if err != nil {
				jsonAPIError(c, http.StatusUnprocessableEntity, errors.Errorf("failed to validate balance: %v", err))
				return
			}
		}
	}

	raw, err := bank.NewMsgSend(tr.FromAddress, tr.DestinationAddress, coins).Marshal()
	if err != nil {
		jsonAPIError(c, http.StatusInternalServerError, errors.Errorf("failed to create message: %v", err))
		return
	}
	enqueuer := chain.MsgEnqueuer()
	msgID, err := enqueuer.Enqueue("", raw)
	if err != nil {
		jsonAPIError(c, http.StatusBadRequest, errors.Errorf("transaction failed: %v", err))
		return
	}
	resource := presenters.NewTerraMsgResource(msgID, tr.TerraChainID, "", raw)
	msgs, err := enqueuer.GetMsgs(msgID)
	if err != nil {
		jsonAPIError(c, http.StatusInternalServerError, errors.Errorf("failed to get message %d: %v", msgID, err))
		return
	}
	if len(msgs) != 1 {
		jsonAPIError(c, http.StatusInternalServerError, errors.Errorf("failed to get message %d: %v", msgID, err))
		return
	}
	msg := msgs[0]
	resource.TxHash = msg.TxHash
	resource.State = string(msg.State)

	jsonAPIResponse(c, resource, "terra_msg")
}

// ValidateTerraBalanceForTransfer validates that the current balance can cover the transaction amount
func ValidateTerraBalanceForTransfer(reader client.Reader, fromAddr sdk.AccAddress, coin sdk.Coin) error {
	balance, err := reader.Balance(fromAddr, coin.Denom)
	if err != nil {
		return err
	}
	//TODO fees?
	if !balance.IsGTE(coin) {
		return errors.Errorf("balance is too low for this transaction to be executed: %v", balance)
	}
	return nil
}
