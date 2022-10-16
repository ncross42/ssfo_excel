package utils

import (
	"fmt"
	"log"
	"time"

	"github.com/xuri/excelize/v2"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type Warning struct {
	Level   string `db:"Level"`
	Code    int    `db:"Code"`
	Message string `db:"Message"`
}

type Visitor struct {
	CarNo  string `db:"car_no"`
	InDate string `db:"in_date"`
	InTime string `db:"in_time"`
	Door   string `db:"door"`
}

type Guest struct {
	CarNo  string `db:"car_no"`
	InDate string `db:"in_date"`
	InTime string `db:"in_time"`
	Door   string `db:"door"`
	Dong   string `db:"dong"`
	Ho     string `db:"ho"`
}

type Entries struct {
	Data        []interface{}
	ColumnIndex map[string]int
}

func (e *Entries) SetColumnIndex(f *excelize.File) int {

	var header_all []string
	rows, err := f.Rows("Sheet1")
	if err != nil {
		fmt.Println(err)
		return 0
	}

	if !rows.Next() {
		fmt.Println("Rows tailed to iterate in GetHeaderFiltered")
		return 0
	}
	row, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	header_all = append(header_all, row...)
	fmt.Println(len(header_all), header_all)

	for row_no, val := range header_all {
		switch val {
		case "입차차량번호":
			e.ColumnIndex["차량번호"] = row_no
		case "입차일자", "입차시각", "차량번호", "입차기기", "동", "호":
			e.ColumnIndex[val] = row_no
		}
	}
	fmt.Println(e.ColumnIndex)
	return len(e.ColumnIndex)
}

func (e *Entries) LoadData(f *excelize.File) int {
	fmt.Println("Start to LoadData()")
	// Rows returns a rows iterator, used for streaming reading data for a worksheet with a large data
	rows, err := f.Rows("Sheet1")
	if err != nil {
		fmt.Println("Failed to f.Rows()")
		fmt.Println(err)
		return 0
	}

	var entry_type = len(e.ColumnIndex)
	fmt.Println("entry_type", entry_type)

	// skip header row
	rows.Next()
	rows.Columns()
	// data row
	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			fmt.Println("Failed to rows.Next()")
			fmt.Println(err)
		}
		if len(row) == 0 {
			break
		}
		if len(row) < 4 {
			fmt.Println("Invalid entry row :", row)
			continue
		}
		switch entry_type {
		case 4:
			e.Data = append(e.Data, Visitor{
				CarNo:  row[e.ColumnIndex["차량번호"]],
				InDate: row[e.ColumnIndex["입차일자"]],
				InTime: row[e.ColumnIndex["입차시각"]],
				Door:   row[e.ColumnIndex["입차기기"]],
			})
		case 6:
			e.Data = append(e.Data, Guest{
				CarNo:  row[e.ColumnIndex["차량번호"]],
				InDate: row[e.ColumnIndex["입차일자"]],
				InTime: row[e.ColumnIndex["입차시각"]],
				Door:   row[e.ColumnIndex["입차기기"]],
				Dong:   row[e.ColumnIndex["동"]],
				Ho:     row[e.ColumnIndex["호"]],
			})
		}
	}

	return len(e.Data)
}

func (e *Entries) InsertData() int64 {

	// use sqlx.Open() for sql.Open() semantics
	db, err := sqlx.Connect("mysql", "root:@(localhost:3306)/fore")
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		// Close the spreadsheet.
		if err := db.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sql := ""
	switch len(e.ColumnIndex) {
	case 4:
		sql = `INSERT IGNORE INTO visitor (car_no, in_date, in_time, door)
		VALUES (:car_no, :in_date, :in_time, :door)`
	case 6:
		sql = `INSERT IGNORE INTO guest (car_no, in_date, in_time, door, dong, ho)
		VALUES (:car_no, :in_date, :in_time, :door, :dong, :ho)`
	}
	r, err := db.NamedExec(sql, e.Data)
	if err != nil {
		log.Fatalln(err)
	}
	in_cnt, err := r.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}

	warn_cnt := len(e.Data) - int(in_cnt)
	fmt.Println(len(e.Data), int(in_cnt), warn_cnt)
	if 0 < warn_cnt {
		// Query the database, storing results in a []Person (wrapped in []interface{})
		warnings := []Warning{}
		if err := db.Select(&warnings, "SHOW WARNINGS"); err != nil {
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

func Importer(file_path string) {

	// if len(os.Args) != 2 {
	// 	fmt.Println("len", len(os.Args))
	// 	for _, arg := range os.Args[1:] {
	// 		fmt.Println(arg)
	// 	}
	// 	fmt.Println("Usage: program {type:g or v} {file_path}")
	// 	return
	// }
	// file_path := os.Args[1]

	PrintMemUsage()
	start := time.Now()

	f, err := excelize.OpenFile(file_path /*, excelize.Options{UnzipXMLSizeLimit: 104857600}*/)
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

	var entries Entries = Entries{nil, make(map[string]int)}
	if entries.SetColumnIndex(f) == 0 {
		fmt.Println("Failed to read header")
		return
	}
	fmt.Println("ColumnIndex", entries.ColumnIndex)

	// Get all the rows in the Sheet1.
	var nData int = entries.LoadData(f)
	fmt.Println("entries count : ", nData)
	if nData < 10 {
		fmt.Println("entries data : ", entries.Data)
	}

	nInserted := entries.InsertData()
	fmt.Println("inserted data count : ", nInserted)

	fmt.Println("execution time", time.Since(start))
	PrintMemUsage()
}
