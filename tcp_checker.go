package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Nodes          []string `json:"nodes"`
	Timeout        float64  `json:"timeout"`
	Probes         int      `json:"probes"`
	MinSuccessRate float64  `json:"min_success_rate"`
	MaxWorkers     int      `json:"max_workers"`
}

type Result struct {
	Node    string  `json:"node"`
	Latency float64 `json:"latency"`
	Country string  `json:"country"`
	Success int     `json:"success"`
}

func testNode(node string, timeout time.Duration, probes int, minSuccessRate float64) *Result {
	hash := strings.LastIndex(node, "#")
	if hash < 0 {
		return nil
	}
	endpoint, country := node[:hash], node[hash+1:]
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || net.ParseIP(host) == nil {
		return nil
	}

	success := 0
	maxLatency := 0.0
	for i := 0; i < probes; i++ {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
		if err != nil {
			continue
		}
		latency := time.Since(start).Seconds()
		_ = conn.Close()
		if latency > maxLatency {
			maxLatency = latency
		}
		success++
	}
	if success == 0 || float64(success)/float64(probes) < minSuccessRate {
		return nil
	}
	return &Result{Node: node, Latency: maxLatency, Country: country, Success: success}
}

func main() {
	var req Request
	if err := json.NewDecoder(bufio.NewReader(os.Stdin)).Decode(&req); err != nil {
		fmt.Fprintln(os.Stderr, "读取 TCP 测试参数失败:", err)
		os.Exit(2)
	}
	if req.Timeout <= 0 {
		req.Timeout = 2
	}
	if req.Probes < 1 {
		req.Probes = 1
	}
	if req.MinSuccessRate < 0 || req.MinSuccessRate > 1 {
		req.MinSuccessRate = 1
	}
	if req.MaxWorkers < 1 {
		req.MaxWorkers = 1000
	}
	if req.MaxWorkers > 10000 {
		req.MaxWorkers = 10000
	}
	if len(req.Nodes) == 0 {
		fmt.Print("[]")
		return
	}

	workers := req.MaxWorkers
	if workers > len(req.Nodes) {
		workers = len(req.Nodes)
	}

	total := len(req.Nodes)
	progressStep := total / 100
	if progressStep < 1 {
		progressStep = 1
	}
	completed := 0
	var progressMu sync.Mutex
	fmt.Fprintf(os.Stderr, "TCP进度：0/%d (0.0%%)\\n", total)

	jobs := make(chan string)
	results := make(chan Result, len(req.Nodes))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for node := range jobs {
				if result := testNode(node, time.Duration(req.Timeout*float64(time.Second)), req.Probes, req.MinSuccessRate); result != nil {
					results <- *result
				}
				progressMu.Lock()
				completed++
				if completed%progressStep == 0 || completed == total {
					fmt.Fprintf(os.Stderr, "TCP进度：%d/%d (%.1f%%)\\n", completed, total, float64(completed)*100/float64(total))
				}
				progressMu.Unlock()
			}
		}()
	}
	go func() {
		for _, node := range req.Nodes {
			jobs <- node
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	out := make([]Result, 0)
	for result := range results {
		out = append(out, result)
	}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "输出 TCP 测试结果失败:", err)
		os.Exit(3)
	}
}
