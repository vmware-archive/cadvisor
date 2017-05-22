
// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wavefront

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	"os"
)

func init() {
	storage.RegisterStorageDriver("wavefront", new)
}

type wavefrontStorage struct {
	Source          string
	ProxyAddress    string
	LastFlush       map[string]time.Time
	Conn            net.Conn
	WfInterval      int
	WfAddTags       string
	WfPrefix        string
	WfTaggifyLabels bool
	WfLabelFilter   []string
	WfDebug		bool
	lock            sync.Mutex
}

const (
	colCpuCumulativeUsage = "cpu_cumulative_usage"
	// Memory Usage
	colMemoryUsage = "memory_usage"
	// Working set size
	colMemoryWorkingSet = "memory_working_set"
	// Cumulative count of bytes received.
	colRxBytes = "rx_bytes"
	// Cumulative count of receive errors encountered.
	colRxErrors = "rx_errors"
	// Cumulative count of bytes transmitted.
	colTxBytes = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	colTxErrors = "tx_errors"
	// Filesystem summary
	colFsSummary = "fs_summary"
	// Filesystem limit.
	colFsLimit = "fs_limit"
	// Filesystem usage.
	colFsUsage = "fs_usage"
)

var (
	argProxyAddress  = flag.String("storage_driver_wf_proxy_host", "", "Wavefront Proxy host:port")
	argPrefix        = flag.String("storage_driver_wf_prefix", "cadvisor.", "Prefix to be added to metrics")
	argInterval      = flag.Int("storage_driver_wf_interval", 60, "Wavefront flush interval")
	argAddTags       = flag.String("storage_driver_wf_add_tags", "", "Point tags to add to metrics")
	argSourceTag     = flag.String("storage_driver_wf_source", "", "Source tag to add to metrics")
	argWfLabelFilter = flag.String("storage_driver_wf_label_filter", "", "A comma separated list of labels that should be taggified")
	argTaggifyLabels = flag.Bool("storage_driver_wf_taggify_labels", true, "If set to true, docker labels will be added as point tags to metrics.")
	argWfDebug 	 = flag.Bool("storage_driver_wf_debug", true, "If set to true, cAdvisor will log all metric lines.")
)

func new() (storage.StorageDriver, error) {
	return newStorage(
		*argProxyAddress,
		*argPrefix,
		*argInterval,
		*argAddTags,
		*argSourceTag,
		*argTaggifyLabels,
		*argWfDebug,
		*argWfLabelFilter,
	)
}

func newStorage(proxyAddress string, prefix string, interval int, addTags string, sourceTag string, taggifyLabels bool, wfDebug bool, labelFilter string) (*wavefrontStorage, error) {

	wavefrontStorage := &wavefrontStorage{
		Source:          sourceTag,
		ProxyAddress:    proxyAddress,
		WfInterval:      interval,
		WfAddTags:       addTags,
		WfPrefix:        prefix,
		WfTaggifyLabels: taggifyLabels,
		WfDebug: 	 wfDebug,
	}


	glog.Infoln("Initializing Wavefront Storage Driver")


	// Parse label filter
	if labelFilter != "" {
		wavefrontStorage.WfLabelFilter = strings.Split(labelFilter, ",")
		glog.Infof("Label filter is set to %s:",labelFilter)
	}

	// Initialize map that will hold timestamp of the last flush for each container
	wavefrontStorage.LastFlush = make(map[string]time.Time)
	// Load environment variables
	if wavefrontStorage.Source == "" {
		return nil, errors.New("Wavefront source tag not set (storage_driver_wf_source_tag param)")
	}
	if wavefrontStorage.ProxyAddress == "" {
		return nil, errors.New("Wavefront proxy address not set (storage_driver_wf_proxy_host param)")
	}
	if wavefrontStorage.WfAddTags == "" {
		//check for environ variable
		tags := os.Getenv("WF_ADD_TAGS")
		if tags != "" {
			wavefrontStorage.WfAddTags = tags
		}
		wavefrontStorage.cleanAddTags()
	}

	return wavefrontStorage, nil

}

func (driver *wavefrontStorage) containerStatsToValues(stats *info.ContainerStats) (series map[string]uint64) {
	series = make(map[string]uint64)

	// Cumulative Cpu Usage
	series[colCpuCumulativeUsage] = stats.Cpu.Usage.Total

	// Memory Usage
	series[colMemoryUsage] = stats.Memory.Usage

	// Working set size
	series[colMemoryWorkingSet] = stats.Memory.WorkingSet

	// Network stats.
	series[colRxBytes] = stats.Network.RxBytes
	series[colRxErrors] = stats.Network.RxErrors
	series[colTxBytes] = stats.Network.TxBytes
	series[colTxErrors] = stats.Network.TxErrors

	return series
}

func (driver *wavefrontStorage) containerFsStatsToValues(series *map[string]uint64, stats *info.ContainerStats) {
	for _, fsStat := range stats.Filesystem {
		// Summary stats.
		(*series)[colFsSummary+"."+colFsLimit] += fsStat.Limit
		(*series)[colFsSummary+"."+colFsUsage] += fsStat.Usage

		// Per device stats.
		(*series)[fsStat.Device+"~"+colFsLimit] = fsStat.Limit
		(*series)[fsStat.Device+"~"+colFsUsage] = fsStat.Usage
	}
}

func (driver *wavefrontStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {

	if stats == nil {
		return nil
	}
	//Container name
	containerName := ref.Name
	if len(ref.Aliases) > 0 {
		containerName = ref.Aliases[0]
	}

	//Only send to WF if the interval has passed.
	current := time.Now()
	dur := current.Sub(driver.LastFlush[containerName])
	//Get the Wavefront interval variable
	//osInterval, err := strconv.Atoi(os.Getenv("WF_INTERVAL"))
	osInterval := driver.WfInterval
	interval := osInterval
	if dur.Seconds() < float64(interval) {
		//it's not time to flush, do nothing
		return nil
	}

	//Open proxy connection
	err := driver.Connect()
	if err != nil {
		glog.Error(fmt.Sprintf("Unable to connect to proxy at %s", driver.ProxyAddress))
		return err
	}
	defer driver.Close()

	//glog.Info("Flushing container stats for " + containerName)
	driver.lock.Lock()
	driver.LastFlush[containerName] = time.Now()
	driver.lock.Unlock()
	//Get current timestamp
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	//Source tag (from flag)
	source := driver.Source

	//do we need to do any replacements?
	if strings.Contains(source,"{") && strings.Contains(source, "}") {
		labelKey := strings.TrimLeft(source,"{")
		labelKey = strings.TrimRight(labelKey,"}")
		labelVal := ref.Labels[labelKey]
		hostname, _ := os.Hostname()
		if labelVal == "" {
			source = hostname
		} else {
			source = labelVal + "-" + hostname
		}
	}
	//See if additional host tags were passed
	addTags := driver.WfAddTags
	//Image
	//Additional tags (namespace and labels)
	appendTags := ""
	//Namespace
	ns := ref.Namespace
	if ns != "" {
		appendTags += " namespace=\"" + ns + "\""
	}
	//Taggify Labels
	if driver.WfTaggifyLabels == true {
		labels := ref.Labels

		// user did not provide a filter include all
		if len(driver.WfLabelFilter) == 0 {
			for key, value := range labels {
				if value != "" {
					appendTags += " " + key + "=\"" + value + "\""
				}
			}
		} else {
			// else only include labels in the filter
			for _,v := range driver.WfLabelFilter {
				if labels[v] != "" {
					appendTags += " " + v + "=\"" + labels[v] + "\""
				}
			}
		}
	}
	//metric data
	series := driver.containerStatsToValues(stats)
	//metric data on volumes
	driver.containerFsStatsToValues(&series, stats)

	for key, value := range series {
		var line string
		if strings.Contains(key, "~") {
			// storage device metrics - extract device as point tag.
			parts := strings.Split(key, "~")
			newKey := parts[1]
			//pointTagVal := strings.Replace(parts[0], "/", "-", -1)
			pointTagVal := parts[0]
			line = fmt.Sprintf("%s%s %v %s source=%s container=\"%s\" device=\"%s\" %s %s \n", driver.WfPrefix, newKey, value, timestamp, source, containerName, pointTagVal, addTags, appendTags)
			fmt.Fprintf(driver.Conn, line)
		} else {
			line = fmt.Sprintf("%s%s %v %s source=%s container=\"%s\" %s %s \n", driver.WfPrefix, key, value, timestamp, source, containerName, addTags, appendTags)
			fmt.Fprintf(driver.Conn, line)
		}
		if driver.WfDebug {
			glog.Infof(line)
		}
	}
	return nil
}

func (driver *wavefrontStorage) cleanAddTags() {
	if strings.HasSuffix(driver.WfAddTags, "\"") {
		driver.lock.Lock()
		driver.WfAddTags = strings.TrimRight(driver.WfAddTags,"\"")
		driver.lock.Unlock()
	}
}

func (driver *wavefrontStorage) Close() error {
	driver.Conn.Close()
	return nil
}

func (driver *wavefrontStorage) Connect() error {
	// Timeout if unable to connect after 10 seconds.
	conn, err := net.DialTimeout("tcp", driver.ProxyAddress, time.Second*10)
	driver.Conn = conn
	return err
}

