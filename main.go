package main

import (
	"fmt"

	"github.com/ncross42/ssfo_excel/utils"

	"github.com/xuri/excelize/v2"
)

func GetHeaderFiltered(rows *excelize.Rows) []int {
	var header_all []string
	if !rows.Next() {
		fmt.Println("Rows tailed to iterate in GetHeaderFiltered")
		return nil
	}
	row, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	header_all = append(header_all, row...)

	fmt.Println(len(header_all), header_all)
	var header_filtered map[int]string = make(map[int]string)
	var header_index []int
	for row_no, val := range header_all {
		switch val {
		case "입차일자", "입차시각", "입차차량번호", "입차기기", "동", "호":
			header_filtered[row_no] = val
			header_index = append(header_index, row_no)
		}
	}
	fmt.Println(header_index, header_filtered)
	return header_index
}

func GetData(f *excelize.File) [][]string {
	// Rows returns a rows iterator, used for streaming reading data for a worksheet with a large data
	rows, err := f.Rows("Sheet1")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	var col_index []int = GetHeaderFiltered(rows)
	var data [][]string = make([][]string, rows.TotalRows()-1)
	row_no := 0
	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			fmt.Println(err)
		}
		for _, col := range col_index {
			data[row_no] = append(data[row_no], row[col])
		}
		// fmt.Println(data[row_no])
		row_no++
	}
	if err = rows.Close(); err != nil {
		fmt.Println(err)
	}
	return data
}

func main() {
	utils.PrintMemUsage()

	f, err := excelize.OpenFile("일반차량 211217 _ 220110.xlsx" /*, excelize.Options{UnzipXMLSizeLimit: 104857600}*/)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		// Close the spreadsheet.
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	// Get all the rows in the Sheet1.
	var data [][]string = GetData(f)
	fmt.Println(len(data))
	// pp.Print(data)

	utils.PrintMemUsage()
}
