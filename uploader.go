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
	"io"
)

type Uploader interface {
	Prepare(bucket string) error
	DoDelete(ctx context.Context, key string) error
	DoDownload(ctx context.Context, key string) (result TransferResult)
	DoUpload(ctx context.Context, key string, data io.ReadSeeker) (result TransferResult)
}
