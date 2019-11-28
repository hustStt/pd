package command

import (
	"github.com/spf13/cobra"
)

var (
	pluginPrefix = "pd/api/v1/newplugin"
	loadPrefix   = "pd/api/v1/newplugin/load"
	updatePrefix = "pd/api/v1/newplugin/update"
	unloadPrefix = "pd/api/v1/newplugin/unload"
)

// NewPluginCommand a set subcommand of plugin command
func NewNPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "newplugin <subcommand>",
		Short: "newplugin commands",
	}
	r.AddCommand(NewLoadNPluginCommand())
	r.AddCommand(NewUpdateNPluginCommand())
	r.AddCommand(NewUnloadNPluginCommand())
	return r
}

// NewLoadPluginCommand return a load subcommand of plugin command
func NewLoadNPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "load <plugin_path>",
		Short: "load a plugin",
		Run:   loadNewPluginCommandFunc,
	}
	return r
}

// NewUpdatePluginCommand return a update subcommand of plugin command
func NewUpdateNPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update <plugin_path>",
		Short: "update plugin",
		Run:   updateNewPluginCommandFunc,
	}
	return r
}

// NewUnloadPluginCommand return a unload subcommand of plugin command
func NewUnloadNPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "unload <plugin_path>",
		Short: "unload a plugin",
		Run:   unloadNewPluginCommandFunc,
	}
	return r
}

func loadNewPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
	}
	postJSON(cmd, loadPrefix, input)
}

func updateNewPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
	}
	postJSON(cmd, updatePrefix, input)
}

func unloadNewPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
	}
	postJSON(cmd, unloadPrefix, input)
}
