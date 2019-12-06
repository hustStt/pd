package main

import (
	"fmt"
	"time"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

func convertToAPIRegions(regions []*core.RegionInfo) *api.RegionsInfo {
	regionInfos := make([]*api.RegionInfo, len(regions))
	for i, r := range regions {
		regionInfos[i] = api.NewRegionInfo(r)
	}
	return &api.RegionsInfo{
		Count:   len(regions),
		Regions: regionInfos,
	}
}

func timetick(ch chan int) {
	tick := time.Tick(time.Minute)
	ch <- 1
    for {
        select {
        case <- tick:
            ch <- 1
        }
    }
}

func GetAll(cluster *server.RaftCluster) {
	regions := cluster.GetRegions()
	regionsInfo := convertToAPIRegions(regions)
	for _,v := range regionsInfo.Regions {
		fmt.Printf("%+v",v)
	}
}

func GetRegions(cluster *server.RaftCluster) {
	ch := make(chan int)

    go timetick(ch)

    for  {
        res := <- ch
        if res == 1 {
            go GetAll(cluster)
        }
    }
}
