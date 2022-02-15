//go:build integration

package cmd_test

import (
	"flag"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	null "gopkg.in/guregu/null.v4"

	terraclient "github.com/smartcontractkit/chainlink-terra/pkg/terra/client"
	terradb "github.com/smartcontractkit/chainlink-terra/pkg/terra/db"

	"github.com/smartcontractkit/chainlink/core/chains/terra/terratxm"
	terratypes "github.com/smartcontractkit/chainlink/core/chains/terra/types"
	"github.com/smartcontractkit/chainlink/core/cmd"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/configtest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/pgtest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/terratest"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/keystore/keys/terrakey"
)

func TestClient_SendTerraCoins(t *testing.T) {
	t.Parallel()

	const localTerrad = "http://127.0.0.1:26657"
	chainID := terratest.RandomChainID()
	accounts, _ := terraclient.SetupLocalTerraNode(t, chainID)
	require.Greater(t, len(accounts), 1)
	app := startNewApplication(t, withConfigSet(func(c *configtest.TestGeneralConfig) {
		c.Overrides.EVMEnabled = null.BoolFrom(false)
		c.Overrides.EVMRPCEnabled = null.BoolFrom(false)
		c.Overrides.TerraEnabled = null.BoolFrom(true)
	}))

	from := accounts[0]
	to := accounts[1]
	require.NoError(t, app.GetKeyStore().Terra().Add(terrakey.Raw(from.PrivateKey.Bytes()).Key()))

	chains := app.GetChains()
	_, err := chains.Terra.Add(chainID, terradb.ChainCfg{})
	require.NoError(t, err)

	_, err = chains.Terra.ORM().CreateNode(terratypes.NewNode{
		Name:          t.Name(),
		TerraChainID:  chainID,
		TendermintURL: localTerrad,
	})
	require.NoError(t, err)

	db := app.GetSqlxDB()
	orm := terratxm.NewORM(chainID, db, logger.TestLogger(t), pgtest.NewPGCfg(true))

	client, r := app.NewClientAndRenderer()
	cliapp := cli.NewApp()

	const tooLow = "balance is too low for this transaction to be executed:"
	for _, tt := range []struct {
		name   string
		coins  string
		expErr string
	}{
		{name: "uluna", coins: "1001uluna"},
		{name: "luna", coins: "1.1luna", expErr: tooLow},
		{name: "multi", coins: "30.235ust,1uluna,50btc", expErr: tooLow},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			set := flag.NewFlagSet("sendterracoins", 0)
			set.String("id", chainID, "")
			set.Parse([]string{tt.coins, from.Address.String(), to.Address.String()})

			c := cli.NewContext(cliapp, set, nil)

			err := client.TerraSendCoins(c)
			if tt.expErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expErr)
			}

			renderedMsg := *r.Renders[0].(*cmd.TerraMsgPresenter)
			require.NotEmpty(t, renderedMsg.ID)
			assert.Equal(t, string(terradb.Unstarted), renderedMsg.State)
			assert.Nil(t, renderedMsg.TxHash)
			id, err := strconv.ParseInt(renderedMsg.ID, 10, 64)
			require.NoError(t, err)
			msgs, err := orm.GetMsgs(id)
			require.NoError(t, err)
			require.Equal(t, 1, len(msgs))
			msg := msgs[0]
			assert.Equal(t, strconv.FormatInt(msg.ID, 10), renderedMsg.ID)
			assert.Equal(t, msg.ChainID, renderedMsg.ChainID)
			assert.Equal(t, msg.ContractID, renderedMsg.ContractID)
			assert.Equal(t, terradb.Unstarted, msg.State)
			assert.Nil(t, msg.TxHash)
		})
	}
}
