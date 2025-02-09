// Copyright 2017 PingCAP, Inc.
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

package command

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	schedulersPrefix             = "pd/api/v1/schedulers"
	schedulerConfigPrefix        = "pd/api/v1/scheduler-config"
	evictLeaderSchedulerName     = "evict-leader-scheduler"
	evictSchedulerHasNoStoreInfo = "No store in evict-leader-scheduler-config"
)

// NewSchedulerCommand returns a scheduler command.
func NewSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scheduler",
		Short: "scheduler commands",
	}
	c.AddCommand(NewShowSchedulerCommand())
	c.AddCommand(NewAddSchedulerCommand())
	c.AddCommand(NewRemoveSchedulerCommand())
	c.AddCommand(NewPauseSchedulerCommand())
	c.AddCommand(NewResumeSchedulerCommand())
	c.AddCommand(NewConfigSchedulerCommand())
	return c
}

// NewPauseSchedulerCommand returns a command to pause a scheduler.
func NewPauseSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "pause <scheduler> <delay>",
		Short: "pause a scheduler",
		Run:   pauseOrResumeSchedulerCommandFunc,
	}
	return c
}

func pauseOrResumeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 && len(args) != 1 {
		cmd.Usage()
		return
	}
	path := schedulersPrefix + "/" + args[0]
	input := make(map[string]interface{})
	input["delay"] = 0
	if len(args) == 2 {
		dealy, err := strconv.Atoi(args[1])
		if err != nil {
			cmd.Usage()
			return
		}
		input["delay"] = dealy
	}
	postJSON(cmd, path, input)
}

// NewShowSchedulerCommand returns a command to show schedulers.
func NewShowSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show",
		Short: "show schedulers",
		Run:   showSchedulerCommandFunc,
	}
	return c
}

// NewResumeSchedulerCommand returns a command to resume a scheduler.
func NewResumeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "resume <scheduler>",
		Short: "resume a scheduler",
		Run:   pauseOrResumeSchedulerCommandFunc,
	}
	return c
}

func showSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

// NewAddSchedulerCommand returns a command to add scheduler.
func NewAddSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "add <scheduler>",
		Short: "add a scheduler",
	}
	c.AddCommand(NewGrantLeaderSchedulerCommand())
	c.AddCommand(NewEvictLeaderSchedulerCommand())
	c.AddCommand(NewShuffleLeaderSchedulerCommand())
	c.AddCommand(NewShuffleRegionSchedulerCommand())
	c.AddCommand(NewShuffleHotRegionSchedulerCommand())
	c.AddCommand(NewScatterRangeSchedulerCommand())
	c.AddCommand(NewBalanceLeaderSchedulerCommand())
	c.AddCommand(NewBalanceRegionSchedulerCommand())
	c.AddCommand(NewBalanceHotRegionSchedulerCommand())
	c.AddCommand(NewRandomMergeSchedulerCommand())
	c.AddCommand(NewBalanceAdjacentRegionSchedulerCommand())
	c.AddCommand(NewLabelSchedulerCommand())
	return c
}

// NewGrantLeaderSchedulerCommand returns a command to add a grant-leader-scheduler.
func NewGrantLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "grant-leader-scheduler <store_id>",
		Short: "add a scheduler to grant leader to a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

// NewEvictLeaderSchedulerCommand returns a command to add a evict-leader-scheduler.
func NewEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "add a scheduler to evict leader from a store",
		Run:   addSchedulerForStoreCommandFunc,
	}
	return c
}

func checkEvicLeaderSchedulerExist(cmd *cobra.Command) (bool, error) {
	r, err := doRequest(cmd, schedulersPrefix, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return false, err
	}
	var scheudlerList []string
	json.Unmarshal([]byte(r), &scheudlerList)
	for idx := range scheudlerList {
		if strings.Contains(scheudlerList[idx], evictLeaderSchedulerName) {
			return true, nil
		}
	}
	return false, nil
}

func addSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	//we should ensure whether it is the first time to create evict-leader-scheduler
	//or just update the evict-leader. But is add one ttl time.
	if evictLeaderSchedulerName == cmd.Name() {
		exist, err := checkEvicLeaderSchedulerExist(cmd)
		if err != nil {
			return
		}
		//if there exist a evict-leader-scheduler we should only update it
		if exist {
			updateConfigSchedulerForStoreCommandFunc(cmd, args)
			return
		}
	}
	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["store_id"] = storeID
	postJSON(cmd, schedulersPrefix, input)
}

// NewShuffleLeaderSchedulerCommand returns a command to add a shuffle-leader-scheduler.
func NewShuffleLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-leader-scheduler",
		Short: "add a scheduler to shuffle leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleRegionSchedulerCommand returns a command to add a shuffle-region-scheduler.
func NewShuffleRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-region-scheduler",
		Short: "add a scheduler to shuffle regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewShuffleHotRegionSchedulerCommand returns a command to add a shuffle-hot-region-scheduler.
func NewShuffleHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "shuffle-hot-region-scheduler [limit]",
		Short: "add a scheduler to shuffle hot regions",
		Run:   addSchedulerForShuffleHotRegionCommandFunc,
	}
	return c
}

func addSchedulerForShuffleHotRegionCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	limit := uint64(1)
	if len(args) == 1 {
		l, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			cmd.Println("Error: ", err)
			return
		}
		limit = l
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["limit"] = limit
	postJSON(cmd, schedulersPrefix, input)
}

// NewBalanceLeaderSchedulerCommand returns a command to add a balance-leader-scheduler.
func NewBalanceLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-leader-scheduler",
		Short: "add a scheduler to balance leaders between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceRegionSchedulerCommand returns a command to add a balance-region-scheduler.
func NewBalanceRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-region-scheduler",
		Short: "add a scheduler to balance regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewBalanceHotRegionSchedulerCommand returns a command to add a balance-hot-region-scheduler.
func NewBalanceHotRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-hot-region-scheduler",
		Short: "add a scheduler to balance hot regions between stores",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewRandomMergeSchedulerCommand returns a command to add a random-merge-scheduler.
func NewRandomMergeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "random-merge-scheduler",
		Short: "add a scheduler to merge regions randomly",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

// NewLabelSchedulerCommand returns a command to add a label-scheduler.
func NewLabelSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "label-scheduler",
		Short: "add a scheduler to schedule regions according to the label",
		Run:   addSchedulerCommandFunc,
	}
	return c
}

func addSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	postJSON(cmd, schedulersPrefix, input)
}

// NewScatterRangeSchedulerCommand returns a command to add a scatter-range-scheduler.
func NewScatterRangeSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "scatter-range [--format=raw|encode|hex] <start_key> <end_key> <range_name>",
		Short: "add a scheduler to scatter range",
		Run:   addSchedulerForScatterRangeCommandFunc,
	}
	c.Flags().String("format", "hex", "the key format")
	return c
}

func addSchedulerForScatterRangeCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 3 {
		cmd.Println(cmd.UsageString())
		return
	}
	startKey, err := parseKey(cmd.Flags(), args[0])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}
	endKey, err := parseKey(cmd.Flags(), args[1])
	if err != nil {
		cmd.Println("Error: ", err)
		return
	}

	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["start_key"] = url.QueryEscape(startKey)
	input["end_key"] = url.QueryEscape(endKey)
	input["range_name"] = args[2]
	postJSON(cmd, schedulersPrefix, input)
}

// NewBalanceAdjacentRegionSchedulerCommand returns a command to add a balance-adjacent-region-scheduler.
func NewBalanceAdjacentRegionSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "balance-adjacent-region-scheduler [leader_limit] [peer_limit]",
		Short: "add a scheduler to disperse adjacent regions on each store",
		Run:   addSchedulerForBalanceAdjacentRegionCommandFunc,
	}
	return c
}

func addSchedulerForBalanceAdjacentRegionCommandFunc(cmd *cobra.Command, args []string) {
	l := len(args)
	input := make(map[string]interface{})
	if l > 2 {
		cmd.Println(cmd.UsageString())
		return
	} else if l == 1 {
		input["leader_limit"] = url.QueryEscape(args[0])
	} else if l == 2 {
		input["leader_limit"] = url.QueryEscape(args[0])
		input["peer_limit"] = url.QueryEscape(args[1])
	}
	input["name"] = cmd.Name()

	postJSON(cmd, schedulersPrefix, input)
}

// NewRemoveSchedulerCommand returns a command to remove scheduler.
func NewRemoveSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <scheduler>",
		Short: "remove a scheduler",
		Run:   removeSchedulerCommandFunc,
	}
	return c
}

func convertReomveSchedulerToRemoveConfig(cmd *cobra.Command, schedulerName string) {
	setCommandUse(cmd, schedulerName)
}

func setCommandUse(cmd *cobra.Command, targetUse string) {
	cmd.Use = targetUse + " "
}

func restoreCommandUse(cmd *cobra.Command, origionCommandUse string) {
	cmd.Use = origionCommandUse
}

func removeSchedulerCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	//FIXME: maybe there is a more graceful method to handler it
	if strings.HasPrefix(args[0], evictLeaderSchedulerName) && args[0] != evictLeaderSchedulerName {
		args = strings.Split(args[0], "-")
		args = args[len(args)-1:]
		cmdStore := cmd.Use
		convertReomveSchedulerToRemoveConfig(cmd, evictLeaderSchedulerName)
		defer restoreCommandUse(cmd, cmdStore)
		deleteConfigSchedulerForStoreCommandFunc(cmd, args)
		return
	}
	path := schedulersPrefix + "/" + args[0]
	_, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		cmd.Println(err)
		return
	}

	cmd.Println("Success!")
}

// NewConfigSchedulerCommand returns commands to config scheduler.
func NewConfigSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "config a scheduler",
	}
	c.AddCommand(NewConfigUpdateCommand())
	c.AddCommand(NewConfigShowCommand())
	c.AddCommand(NewConfigDeleteCommand())
	return c
}

//NewConfigUpdateCommand return a command to update config
func NewConfigUpdateCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "update <scheduler>",
		Short: "update a scheduler",
	}
	c.AddCommand(NewConfigUpdateEvictLeaderSchedulerCommand())
	return c
}

//NewConfigShowCommand return a command to show config of scheduler
func NewConfigShowCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "show <scheduler>",
		Short: "show a scheduler's config",
	}
	c.AddCommand(NewConfigShowEvictLeaderSchedulerCommand())
	return c
}

//NewConfigDeleteCommand return a command to delete config
func NewConfigDeleteCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "delete <scheduler>",
		Short: "delete a scheduler's config",
	}
	c.AddCommand(NewConfigDeleteEvictLeaderSchedulerCommand())
	return c
}

//NewConfigUpdateEvictLeaderSchedulerCommand return a command to config evict-leader-scheduler
func NewConfigUpdateEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "make the scheduler to evict leader from a store",
		Run:   updateConfigSchedulerForStoreCommandFunc,
	}
	return c
}

//NewConfigShowEvictLeaderSchedulerCommand return a command to config evict-leader-scheduler
func NewConfigShowEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler",
		Short: "show the config of evict-leader-scheduler",
		Run:   showConfigSchedulerForStoreCommandFunc,
	}
	return c
}

//NewConfigDeleteEvictLeaderSchedulerCommand delete a config for store_id
func NewConfigDeleteEvictLeaderSchedulerCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict-leader-scheduler <store_id>",
		Short: "delete the config of evict-leader-scheduler",
		Run:   deleteConfigSchedulerForStoreCommandFunc,
	}
	return c
}

func updateConfigSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	storeID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		cmd.Println(err)
		return
	}
	input := make(map[string]interface{})
	input["name"] = cmd.Name()
	input["store_id"] = storeID

	postJSON(cmd, path.Join(schedulerConfigPrefix, cmd.Name(), "config"), input)
}

func showConfigSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Println(cmd.UsageString())
		return
	}
	path := path.Join(schedulerConfigPrefix, cmd.Name(), "list")
	r, err := doRequest(cmd, path, http.MethodGet)
	if err != nil {
		cmd.Println(err)
		return
	}
	cmd.Println(r)
}

//convertReomveConfigToReomveScheduler make cmd can be used at removeCommandFunc
func convertReomveConfigToReomveScheduler(cmd *cobra.Command) {
	setCommandUse(cmd, "remove")
}

func deleteConfigSchedulerForStoreCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.Usage())
		return
	}
	path := path.Join(schedulerConfigPrefix, "/", cmd.Name(), "delete", args[0])
	resp, err := doRequest(cmd, path, http.MethodDelete)
	if err != nil {
		cmd.Println(err)
		return
	}
	//FIXME: remove the judge when the new command replace old command
	if strings.Contains(resp, evictSchedulerHasNoStoreInfo) {
		args = append(args[:0], evictLeaderSchedulerName)
		cmdStore := cmd.Use
		convertReomveConfigToReomveScheduler(cmd)
		defer restoreCommandUse(cmd, cmdStore)
		removeSchedulerCommandFunc(cmd, args)
		return
	}
	cmd.Println("Success!")
}
