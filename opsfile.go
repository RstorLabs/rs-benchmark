package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

type listElement struct {
	key  string
	put  bool
	size int
}

func readOpsCsv(
	ctx context.Context,
	listfile *os.File,
	elements chan<- listElement,
) error {
	_, err := listfile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("can't seek to beginning: %w", err)
	}

	csvReader := csv.NewReader(listfile)

	readElement := func() (listElement, error) {
		record, err := csvReader.Read()
		if err != nil {
			return listElement{}, err
		}

		// for simulations
		if len(record) == 3 {
			size, _ := strconv.Atoi(record[2])
			return listElement{
				key:  record[1],
				put:  record[0] == "PUT",
				size: size,
			}, nil
		}

		return listElement{}, fmt.Errorf("invalid element: %v", record)
	}

	for {
		el, err := readElement()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		select {
		case elements <- el:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
