/*
# rs-benchmark - A utility to benchmark object storages
# Copyright (C) 2016-2019 RStor Inc (open-source@rstor.io)
#
# This file is part of rs-benchmark.
#
# rs-benchmark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# rs-benchmark is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Copyright Header.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

type http2Setting int

const (
	http2_auto  http2Setting = 0
	http2_off   http2Setting = 1
	http2_force http2Setting = 2
)

func makeHttpClient(h2s http2Setting, forceAddress string) (*http.Client, error) {
	var dialer = &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	dialContext := dialer.DialContext

	if forceAddress != "" {
		dialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, forceAddress)
		}
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,

		DialContext: dialContext,

		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 0,

		ResponseHeaderTimeout: 10 * time.Second,

		// Allow an unlimited number of idle connections
		MaxIdleConnsPerHost: 4096,
		MaxIdleConns:        0,

		// But limit their idle time
		IdleConnTimeout: time.Minute,
		MaxConnsPerHost: 0,

		ForceAttemptHTTP2: h2s != http2_off,

		// Ignore TLS errors
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
		DisableCompression: true,
	}

	if h2s == http2_force {
		transport.TLSClientConfig.NextProtos = []string{"h2"}
	}

	return &http.Client{
		Transport: transport,
		Timeout:   time.Minute * 5,
	}, nil
}
