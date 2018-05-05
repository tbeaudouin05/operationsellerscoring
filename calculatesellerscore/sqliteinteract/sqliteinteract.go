package sqliteinteract

import (
	"database/sql"
	"log"
	"time"

	"github.com/joho/sqltocsv"
	"github.com/thomas-bamilo/operationsellerscoring/supplierscorerow"
)

// CreateTtrRfcInboundScoreRtsTable creates and output the ttr, rfc, inbound and rts scores of each supplier
func CreateTtrRfcInboundScoreRtsTable(dbSQLite *sql.DB, ttrRfcTable []supplierscorerow.SupplierScoreRow, inboundScoreTable []supplierscorerow.SupplierScoreRow) {

	createTtrRfcTable(dbSQLite, ttrRfcTable)

	createInboundScoreTable(dbSQLite, inboundScoreTable)

	createTtrRfcInboundView(dbSQLite)

	joinTable(dbSQLite)

}

// create the SQLite table ttr_rfc with the data from ttrRfcTable, an array of SupplierScoreRow
func createTtrRfcTable(db *sql.DB, ttrRfcTable []supplierscorerow.SupplierScoreRow) {

	// create ttr_rfc table
	createTtrRfcTableStr := `CREATE TABLE ttr_rfc (
	year_month INTEGER
	,supplier_name TEXT
	,id_supplier INTEGER
	,avg_ttr_day REAL
	,rfc_score REAL)`

	createTtrRfcTable, err := db.Prepare(createTtrRfcTableStr)
	checkError(err)
	createTtrRfcTable.Exec()

	// insert values into ttr_rfc table
	insertTtrRfcTableStr := `INSERT INTO ttr_rfc (
	year_month
	,supplier_name
	,id_supplier
	,avg_ttr_day
	,rfc_score) 
	VALUES (?, ?, ?, ?, ?)`
	insertTtrRfcTable, err := db.Prepare(insertTtrRfcTableStr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(ttrRfcTable); i++ {
		insertTtrRfcTable.Exec(ttrRfcTable[i].YearMonth,
			ttrRfcTable[i].SupplierName,
			ttrRfcTable[i].IDSupplier,
			ttrRfcTable[i].AvgTtrDay,
			ttrRfcTable[i].RfcScore,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

// create the SQLite table inbound_score with the data from inboundScoreTable, an array of SupplierScoreRow
func createInboundScoreTable(db *sql.DB, inboundScoreTable []supplierscorerow.SupplierScoreRow) {

	// create inbound_score table
	createInboundScoreTableStr := `CREATE TABLE inbound_score (
	year_month INTEGER
	,supplier_name TEXT
	,id_supplier INTEGER
	,inbound_score REAL)`

	createInboundScoreTable, err := db.Prepare(createInboundScoreTableStr)
	checkError(err)
	createInboundScoreTable.Exec()

	// insert values into inbound_score table
	insertInboundScoreTableStr := `INSERT INTO inbound_score (
	year_month
	,supplier_name
	,id_supplier
	,inbound_score) 
	VALUES (?,?,?,?)`
	insertInboundScoreTable, err := db.Prepare(insertInboundScoreTableStr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(inboundScoreTable); i++ {
		insertInboundScoreTable.Exec(
			inboundScoreTable[i].YearMonth,
			inboundScoreTable[i].SupplierName,
			inboundScoreTable[i].IDSupplier,
			inboundScoreTable[i].InboundScore,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

// create the SQLite table rts with the data from rtsTable, an array of SupplierScoreRow
func createRtsTable(db *sql.DB, rtsTable []supplierscorerow.SupplierScoreRow) {

	// create rts table
	createRtsTableStr := `CREATE TABLE rts (
	year_month INTEGER
	,supplier_name TEXT
	,id_supplier INTEGER
	,rts_score REAL)`

	createRtsTable, err := db.Prepare(createRtsTableStr)
	checkError(err)
	createRtsTable.Exec()

	// insert values into rts table
	insertRtsTableStr := `INSERT INTO rts (
	year_month
	,supplier_name
	,id_supplier
	,rts_score) 
	VALUES (?,?,?,?)`
	insertRtsTable, err := db.Prepare(insertRtsTableStr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(rtsTable); i++ {
		insertRtsTable.Exec(
			rtsTable[i].YearMonth,
			rtsTable[i].SupplierName,
			rtsTable[i].IDSupplier,
			rtsTable[i].RtsScore,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

// since SQLite does not have full outer join....... we need to build it ourselves with left join + union all....
// which requires to build intermediate views if more than two tables need to be joined :(
func createTtrRfcInboundView(db *sql.DB) {

	// create iss_tr view
	createTtrRfcInboundViewStr := `
	CREATE VIEW iss_tr AS
	SELECT
	COALESCE(tr.year_month,iss.year_month) 'year_month'
	,COALESCE(tr.supplier_name,iss.supplier_name) 'supplier_name'
	,COALESCE(tr.id_supplier,iss.id_supplier) 'id_supplier'
	,COALESCE(tr.avg_ttr_day,0) 'avg_ttr_day'
	,COALESCE(tr.rfc_score,0) 'rfc_score'
	,COALESCE(iss.inbound_score,0) 'inbound_score'
   	FROM ttr_rfc tr
   	LEFT JOIN inbound_score iss USING(id_supplier)
	   UNION ALL
	SELECT
	COALESCE(tr.year_month,iss.year_month) 'year_month'
	,COALESCE(tr.supplier_name,iss.supplier_name) 'supplier_name'
	,COALESCE(tr.id_supplier,iss.id_supplier) 'id_supplier'
	,COALESCE(tr.avg_ttr_day,0) 'avg_ttr_day'
	,COALESCE(tr.rfc_score,0) 'rfc_score'
	,COALESCE(iss.inbound_score,0) 'inbound_score'
	FROM inbound_score iss
	LEFT JOIN ttr_rfc tr USING(id_supplier)
	WHERE tr.year_month IS NULL`

	createTtrRfcInboundView, err := db.Prepare(createTtrRfcInboundViewStr)
	checkError(err)
	createTtrRfcInboundView.Exec()

}

// full outer join the SQLite tables iss_tr = TtrRfcInboundView and rts
// and output the result into csv
func joinTable(db *sql.DB) {
	// store the query in a string
	query := `
	SELECT
	COALESCE(iss_tr.year_month,rts.year_month) 'year_month'
	,COALESCE(iss_tr.supplier_name,rts.supplier_name) 'supplier_name'
	,COALESCE(iss_tr.id_supplier,rts.id_supplier) 'id_supplier'
	,COALESCE(iss_tr.avg_ttr_day,0) 'avg_ttr_day'
	,COALESCE(iss_tr.rfc_score,0) 'rfc_score'
	,COALESCE(iss_tr.inbound_score,0) 'inbound_score'
	,COALESCE(rts.rts_score,0) 'rts_score'
   	FROM iss_tr
   	LEFT JOIN rts USING(id_supplier)
	   UNION ALL
	SELECT
	COALESCE(iss_tr.year_month,rts.year_month) 'year_month'
	,COALESCE(iss_tr.supplier_name,rts.supplier_name) 'supplier_name'
	,COALESCE(iss_tr.id_supplier,rts.id_supplier) 'id_supplier'
	,COALESCE(iss_tr.avg_ttr_day,0) 'avg_ttr_day'
	,COALESCE(iss_tr.rfc_score,0) 'rfc_score'
	,COALESCE(iss_tr.inbound_score,0) 'inbound_score'
	FROM rts
	LEFT JOIN iss_tr USING(id_supplier)
	WHERE iss_tr.year_month IS NULL`

	var yearMonth, supplierName, iDSupplier string
	var avgTtrDay, rfcScore, inboundScore, rtsScore float32
	var supplierScoreTable []supplierscorerow.SupplierScoreRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&yearMonth, &supplierName, &iDSupplier, &avgTtrDay, &rfcScore, &inboundScore, &rtsScore)
		if err != nil {
			log.Fatal(err)
		}
		supplierScoreTable = append(supplierScoreTable,
			supplierscorerow.SupplierScoreRow{
				YearMonth:    yearMonth,
				SupplierName: supplierName,
				IDSupplier:   iDSupplier,
				AvgTtrDay:    avgTtrDay,
				RfcScore:     rfcScore,
				InboundScore: inboundScore,
				RtsScore:     rtsScore,
			})
		err = sqltocsv.WriteFile("supplierscore.csv", rows)
		checkError(err)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

/*func changeDateType(dateTimeStrIn string) string {

	dateTimeParsed, e := time.Parse(dateTimeStrIn, "1/2/2006 15:04:05")
	return string(dateTimeParsed.Format("2006-01-02 15:04:05"))

}*/
