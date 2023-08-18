package cli

import (
	"fmt"

	apiv1 "github.com/acorn-io/runtime/pkg/apis/api.acorn.io/v1"
	cli "github.com/acorn-io/runtime/pkg/cli/builder"
	"github.com/acorn-io/runtime/pkg/client"
	"github.com/acorn-io/runtime/pkg/config"
	credentials2 "github.com/acorn-io/runtime/pkg/credentials"
	"github.com/spf13/cobra"
	"k8s.io/utils/strings/slices"
)

func NewCredentialLogout(root bool, c CommandContext) *cobra.Command {
	cmd := cli.Command(&CredentialLogout{client: c.ClientFactory}, cobra.Command{
		Use:     "logout [flags] [SERVER_ADDRESS]",
		Aliases: []string{"rm"},
		Example: `
acorn logout ghcr.io`,
		SilenceUsage:      true,
		Short:             "Remove registry credentials",
		ValidArgsFunction: newCompletion(c.ClientFactory, credentialsCompletion).withShouldCompleteOptions(onlyNumArgs(1)).complete,
	})
	if root {
		cmd.Aliases = nil
	}
	return cmd
}

type CredentialLogout struct {
	client       ClientFactory
	LocalStorage bool `usage:"Delete locally stored credential (not remotely stored)" short:"l"`
}

func (a *CredentialLogout) Run(cmd *cobra.Command, args []string) error {
	cfg, err := a.client.Options().CLIConfig()
	if err != nil {
		return err
	}

	if len(args) == 0 {
		args = []string{cfg.GetDefaultAcornServer()}
	}

	var client client.Client
	if slices.Contains(cfg.AcornServers, args[0]) {
		// force local storage for known manager addresses
		a.LocalStorage = true
	}

	if !a.LocalStorage {
		client, err = a.client.CreateDefault()
		if err != nil {
			return err
		}
	}

	store, err := credentials2.NewStore(cfg, client)
	if err != nil {
		return err
	}

	err = store.Remove(cmd.Context(), apiv1.Credential{
		ServerAddress: args[0],
		LocalStorage:  a.LocalStorage,
	})
	if err != nil {
		return err
	}

	// reload config
	cfg, err = a.client.Options().CLIConfig()
	if err != nil {
		return fmt.Errorf("failed to remove server %s from CLI config: %v", args[0], err)
	}

	return config.RemoveServer(cfg, args[0])
}
