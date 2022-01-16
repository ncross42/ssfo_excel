package main

import (
	"fmt"
	"log"

	"github.com/ncross42/ssfo_excel/utils"

	"github.com/xuri/excelize/v2"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type Warning struct {
	Level   string
	Code    int
	Message string
}

type Visitor struct {
	CarNo  string `db:"car_no"`
	InDate string `db:"in_date"`
	InTime string `db:"in_time"`
	Door   string `db:"door"`
}

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

func GetHeaderFiltered2(rows *excelize.Rows) map[string]int {
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
	var header_filtered map[string]int = make(map[string]int)
	for row_no, val := range header_all {
		switch val {
		case "입차일자", "입차시각", "입차차량번호", "입차기기", "동", "호":
			header_filtered[val] = row_no
		}
	}
	fmt.Println(header_filtered)
	return header_filtered
}

func GetData2(f *excelize.File) []Visitor {
	// Rows returns a rows iterator, used for streaming reading data for a worksheet with a large data
	rows, err := f.Rows("Sheet1")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	var col_index map[string]int = GetHeaderFiltered2(rows)
	var data []Visitor // = make([]Visitor, rows.TotalRows()-1)
	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			fmt.Println(err)
		}
		// for _, col := range col_index {
		// 	data[row_no] = append(data[row_no], row[col])
		// }
		data = append(data, Visitor{
			CarNo:  row[col_index["입차차량번호"]],
			InDate: row[col_index["입차일자"]],
			InTime: row[col_index["입차시각"]],
			Door:   row[col_index["입차기기"]],
		})
		// fmt.Println(data[row_no])
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
	// var data [][]string = GetData(f)
	var data []Visitor = GetData2(f)
	fmt.Println(len(data))
	if len(data) < 10 {
		fmt.Println(data)
	}

	in_cnt := InsertData(data)
	fmt.Println(in_cnt)

	utils.PrintMemUsage()
}

func InsertData(visitors []Visitor) int64 {

	// use sqlx.Open() for sql.Open() semantics
	db, err := sqlx.Connect("mysql", "root:itsme1@(localhost:3306)/fore")
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		// Close the spreadsheet.
		if err := db.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	r, err := db.NamedExec(`INSERT IGNORE INTO visitor (car_no, in_date, in_time, door)
		VALUES (:car_no, :in_date, :in_time, :door)`, visitors)
	if err != nil {
		log.Fatalln(err)
	}
	in_cnt, err := r.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}

	warn_cnt := len(visitors) - int(in_cnt)
	fmt.Println(warn_cnt)
	if 0 < warn_cnt {
		// Query the database, storing results in a []Person (wrapped in []interface{})
		warnings := []Warning{}
		if err := db.Get(&warnings, "SHOW WARNINGS"); err != nil {
			fmt.Println(err)
		}
		if warn_cnt < 10 {
			fmt.Println(warnings)
		} else {
			fmt.Println("warnings cnt :", warn_cnt)
		}
	}

	return in_cnt
}
