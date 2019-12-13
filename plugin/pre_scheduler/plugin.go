package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"
	"strings"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

//table info
type PreTableInfo struct {
	Predict        []float64 `json:"predict"`
	StartKey       string    `json:"start_key"`
	EndKey         string    `json:"end_key"`
	MaxValue       float64   `json:"max_value"`
	MinValue       float64   `json:"min_value"`
	HistoryR2Score float64   `json:"history_r2_score"`
}

//json
type PredictInfo struct {
	Time                int64          `json:"time"`
	TableNum            int            `json:"table_num"`
	PredictStep         int            `json:"predict_step"`
	HistoryR2ScoreTotal float64        `json:"history_r2_score_tot"`
	TableInfo           []PreTableInfo `json:"table_info"`
}

type HotSpotPeriod struct {
	StartTime int64
	EndTime   int64
	TableKey  []int
}

var gLastPeriod HotSpotPeriod

type ScheduleTime struct {
	StartTime int64   //开始调度时机
	EndTime   int64   //停止维持时间
	DeltT     []int64 //对于所有应用（table）的一个delt t
}

type DispatchTiming struct {
	url           string
	hotDergee     float64
	data          PredictInfo
	hot_spot_arr  []HotSpotPeriod
	period        []HotSpotPeriod
	schedule_time []ScheduleTime
}

type RegionStat struct {
	count    uint64
	regionId []uint64
}

func (p *PredictInfo) GetTableIndexByRegion(meta *metapb.Region) int {
	tableInfo := p.TableInfo
	str := string(core.HexRegionKey(meta.GetStartKey()))
	for index,t := range tableInfo {
		if strings.Compare(t.StartKey,str) <=0 && (t.EndKey == "" || strings.Compare(t.EndKey,str) >0) {
			return index
		}
	}
	return 1
}

func BubbleSort(values []float64) {
	for i := 0; i < len(values)-1; i++ {
		flag := true
		for j := 0; j < len(values)-i-1; j++ {
			if values[j] < values[j+1] {
				values[j], values[j+1] = values[j+1], values[j]
				flag = false
			}
		}
		if flag == true {
			break
		}
	}
}

func BubbleSortHotSpot(values []HotSpotPeriod) {
	for i := 0; i < len(values)-1; i++ {
		flag := true
		for j := 0; j < len(values)-i-1; j++ {
			if values[j].StartTime > values[j+1].StartTime {
				values[j], values[j+1] = values[j+1], values[j]
				flag = false
			}
		}
		if flag == true {
			break
		}
	}
}

func (d *DispatchTiming) setUrl(s string) {
	d.url = s
}

func (d *DispatchTiming) getPredictInfo() {
	if d.url == "" {
		return
	}
	fmt.Println(d.url)
	resp, err := http.Get(d.url)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("read body err, %v\n", err)
		return
	}
	fmt.Println("json:", string(body))

	err2 := json.Unmarshal(body, &d.data)
	if err2 != nil {
		fmt.Printf("unmarshell err, %v\n", err2)
		return
	}
	fmt.Printf("%+v", d.data)
}

func (d *DispatchTiming) getHotDergee() {
	info := d.data
	if info.PredictStep <= 0 || info.TableNum <= 0 {
		fmt.Printf("predictstep or tablenum err\n")
		return
	}
	preDataTotal := make([]float64, info.PredictStep)
	for i := 0; i < info.PredictStep; i++ {
		preDataTotal[i] = 0
		for j := 0; j < info.TableNum; j++ {
			preDataTotal[i] += info.TableInfo[j].Predict[i]
		}
		preDataTotal[i] = preDataTotal[i] / float64(info.TableNum)
	}
	BubbleSort(preDataTotal)

	ave := (preDataTotal[info.PredictStep/5] + preDataTotal[info.PredictStep/5+1]) / 2
	d.hotDergee = ave
}

func (d *DispatchTiming) getHotSpot() {
	info := d.data
	hotPoint := make([][]bool, info.TableNum)
	for i := 0; i < info.TableNum; i++ {
		hotPoint[i] = make([]bool, info.PredictStep)
	}

	//d.hot_spot_arr = make([]HotSpotPeriod, 0, info.TableNum*2)
	for i := 0; i < info.TableNum; i++ {
		flag := false
		var start_t int64
		var end_t int64
		for j := 0; j < info.PredictStep; j++ {
			if info.TableInfo[i].Predict[j] > d.hotDergee {
				hotPoint[i][j] = true
			} else {
				hotPoint[i][j] = false
			}
			if hotPoint[i][j] == true && flag == false {
				flag = true
				start_t = int64(j+1)*60 + info.Time
			}
			if (hotPoint[i][j] == false && flag == true) || (hotPoint[i][j] == true && flag == true && j == info.PredictStep-1) {
				flag = false
				end_t = int64(j)*60 + info.Time
				tmp := HotSpotPeriod{start_t, end_t, []int{i}}
				d.hot_spot_arr = append(d.hot_spot_arr, tmp)
			}
		}
	}
	if len(d.hot_spot_arr) <= 0 {
		fmt.Printf("no hot spot\n")
		return
	}
	BubbleSortHotSpot(d.hot_spot_arr)
	fmt.Println(hotPoint)
	fmt.Println(d.hot_spot_arr)
}

func (d *DispatchTiming) getPeriod() {
	//period := make([]HotSpotPeriod, 0, info.TableNum*2)
	table_arr := []int{}
	d.period = append(d.period, gLastPeriod)

	p := 0
	q := 0
	//table_arr = append(table_arr,hot_spot_arr[0].TableKey...)
	start_t := d.hot_spot_arr[0].StartTime
	end_t := d.hot_spot_arr[q].EndTime
	for {
		if d.hot_spot_arr[q].StartTime-end_t > 4*60 {
			tmp := HotSpotPeriod{start_t, end_t, table_arr}
			d.period = append(d.period, tmp)
			start_t = d.hot_spot_arr[q].StartTime
			end_t = d.hot_spot_arr[q].EndTime
			table_arr = append([]int{})
			p = q
		} else {
			if d.hot_spot_arr[q].EndTime > d.hot_spot_arr[p].EndTime {
				end_t = d.hot_spot_arr[q].EndTime
			}
			table_arr = append(table_arr, d.hot_spot_arr[q].TableKey...)
			q++
		}
		if q == len(d.hot_spot_arr) {
			for _, v := range d.period {
				if v.StartTime == start_t && v.EndTime == end_t {
					break
				}
			}
			tmp := HotSpotPeriod{start_t, end_t, table_arr}
			d.period = append(d.period, tmp)
			break
		}
	}
	fmt.Println(d.period)
}

func (d *DispatchTiming) getTiming() {
	//schedule_time := make([]ScheduleTime, 0, info.TableNum)
	info := d.data
	for i := 1; i < len(d.period); i++ {
		//调度时机
		tmp := (d.period[i].StartTime + d.period[i-1].EndTime) / 2
		//如果间隔超过xx 即可划分  这里主要是处理跨周期的热点
		if d.period[i].StartTime-d.period[i-1].EndTime > 4*60 {
			//如果一开始就是高负载 则立即开始调度
			if d.period[i-1].EndTime == 0 {
				tmp = info.Time
			}
			arr := make([]int64, info.TableNum)
			tmp_t := ScheduleTime{tmp, d.period[i].EndTime, arr}
			d.schedule_time = append(d.schedule_time, tmp_t)
		}
	}

	for _, v := range d.schedule_time {
		for _, h := range d.hot_spot_arr {
			tmp := (h.StartTime + h.EndTime) / 2
			//如果在该调度时期内
			if tmp >= v.StartTime && tmp <= v.EndTime {
				v.DeltT[h.TableKey[0]] = 1
			} else if tmp > v.EndTime {
				t := (tmp - v.StartTime) / 60
				if v.DeltT[h.TableKey[0]] == 0 || t < v.DeltT[h.TableKey[0]] {
					v.DeltT[h.TableKey[0]] = t
				}
			} else {
				var t int64
				tfloat := float64((tmp - v.StartTime) / 60)
				t = int64(math.Floor(math.Pow(tfloat, 2)))

				if v.DeltT[h.TableKey[0]] == 0 || t < v.DeltT[h.TableKey[0]] {
					v.DeltT[h.TableKey[0]] = t
				}
			}
		}
	}
	fmt.Println(d.schedule_time)
	//将周期最后一个综合高负载时间段记录下来
	gLastPeriod = d.period[len(d.period)-1]
}

func newDispatchTiming(ch chan DispatchTiming) {
	var disp DispatchTiming
	disp.setUrl("http://192.168.140.160:8888/")
	disp.getPredictInfo()
	disp.getHotDergee()
	disp.getHotSpot()
	disp.getPeriod()
	disp.getTiming()
	ch <- disp
}


func GetTopK(cluster *server.RaftCluster,d DispatchTiming,index int) {
	DeltT := d.schedule_time[index].DeltT
	info := d.data
	regions := cluster.GetRegions()
	if len(regions) <= 0 {
		fmt.Println("not found region")
		return
	}
	minrw := regions[0].GetRwBytesTotal()/uint64(DeltT[info.GetTableIndexByRegion(regions[0].GetMeta())])
	maxrw := regions[0].GetRwBytesTotal()/uint64(DeltT[info.GetTableIndexByRegion(regions[0].GetMeta())])
	tmp := make([]uint64,len(regions))
	for index, v := range regions {
		tmp[index] = v.GetRwBytesTotal()/uint64(DeltT[info.GetTableIndexByRegion(v.GetMeta())])
		if minrw > tmp[index] {
			minrw = tmp[index]
		}
		if maxrw < tmp[index] {
			maxrw = tmp[index]
		}
	}
	segment := (maxrw - minrw) / uint64(len(regions))
	if segment == 0 {
		segment = 1
	}
	HotDegree := make([]RegionStat, len(regions)+1)
	for index, v := range regions {
		data := tmp[index]
		index := (data - minrw) / segment
		HotDegree[index].count++
		HotDegree[index].regionId = append(HotDegree[index].regionId, v.GetID())
	}
	k := 0
	topk := len(regions)/10
	var retRegionId []uint64
	for _,h := range HotDegree {
		for _,i := range h.regionId {
			retRegionId = append(retRegionId, i)
			k++
			if k == topk {
				break
			}
		}
	}
	fmt.Println(retRegionId)
}


func timetick(ch chan int) {
	tick := time.Tick(time.Hour)
	ch <- 1
	for {
		select {
		case <-tick:
			ch <- 1
		}
	}
}

func processDispatchTiming(chd chan DispatchTiming) {
	ch := make(chan int)
	go timetick(ch) //定时

	for {
		res := <-ch
		if res == 1 {
			newDispatchTiming(chd)
		}
	}
}

func processTopK(cluster *server.RaftCluster,d DispatchTiming) {
	scheduleTime := d.schedule_time

	for index := 0;index < len(scheduleTime);index++ {
		now := time.Now()
		fmt.Println(scheduleTime[index].StartTime)
		next := time.Unix(now.Unix() + 30 * int64(index),0)
		t := time.NewTimer(next.Sub(now))
		<-t.C
		GetTopK(cluster,d,index)
	}
}

func Mytest(cluster *server.RaftCluster) {
	chd := make(chan DispatchTiming)
	go processDispatchTiming(chd)

	for {
		res := <-chd
		go processTopK(cluster,res)
	}
}
