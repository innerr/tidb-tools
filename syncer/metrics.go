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

package main

import (
	"fmt"
	"net/http"
	"strings"
	// For pprof
	_ "net/http/pprof"
	"strconv"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	binlogEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_events_total",
			Help:      "total number of binlog events",
		}, []string{"type"})

	binlogSkippedEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_skipped_events_total",
			Help:      "total number of skipped binlog events",
		}, []string{"type"})

	sqlJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_jobs_total",
			Help:      "total number of sql jobs",
		}, []string{"type"})

	sqlRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retryies",
		}, []string{"type"})

	binlogPos = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"type"})

	binlogFile = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"type"})

	binlogGTID = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "gtid",
			Help:      "current transaction id",
		}, []string{"server_uuid"})
)

func initStatusAndMetrics(addr string) {
	prometheus.MustRegister(binlogEventsTotal)
	prometheus.MustRegister(binlogSkippedEventsTotal)
	prometheus.MustRegister(sqlJobsTotal)
	prometheus.MustRegister(sqlRetriesTotal)
	prometheus.MustRegister(binlogPos)
	prometheus.MustRegister(binlogFile)
	prometheus.MustRegister(binlogGTID)

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "application/text")
			text := GetRawSyncerInfo()
			w.Write([]byte(text))
		})

		// HTTP path for prometheus.
		http.Handle("/metrics", prometheus.Handler())
		log.Infof("listening on %v for status and metrics report.", addr)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func getBinlogIndex(filename string) float64 {
	spt := strings.Split(filename, ".")
	if len(spt) == 1 {
		log.Warnf("[syncer] invalid binlog file: %s", filename)
		return 0
	}
	idxStr := spt[len(spt)-1]

	idx, err := strconv.ParseFloat(idxStr, 64)
	if err != nil {
		log.Warnf("[syncer] parse binlog index %s, error %s", filename, err.Error())
		return 0
	}
	return idx
}

func masterGTIDGauge(gtidSet GTIDSet) {
	for uuid, uuidSet := range gtidSet.all() {
		length := uuidSet.Intervals.Len()
		maxStop := uuidSet.Intervals[length-1].Stop
		binlogGTID.WithLabelValues(fmt.Sprintf("master_binlog_gtid_%s", uuid)).Set(float64(maxStop))
	}
}
