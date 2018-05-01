package biqueryttrrfc

import (
	"database/sql"
	"log"

	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"

	"github.com/thomas-bamilo/operationsellerscoring/supplierscorerow"
)

// CreateTtrRfcTable queries bi_database to create an array of supplierscorerow.SupplierScoreRow which represent TtrRfcTable, the table which records:
// "time to respond" and "return from customer score" for each seller on a year_month basis
func CreateTtrRfcTable(db *sql.DB) []supplierscorerow.SupplierScoreRow {

	// store the query in a string
	query := `SELECT
		  CONCAT(
	  CASE WHEN MONTH(GETDATE()) = 1 
	  THEN YEAR(GETDATE())-1 
	  ELSE YEAR(GETDATE()) END, 
	  CASE WHEN MONTH(GETDATE()) = 1 
	  THEN 12 
	  ELSE MONTH(GETDATE())-1 END
	  ) 'year_month'
	  ,COALESCE(ttr.supplier_name,rfc.supplier_name) 'supplier_name'
	  ,COALESCE(ttr.id_supplier,rfc.id_supplier) 'id_supplier'
	  ,COALESCE(ttr.avg_ttr_day,0) 'avg_ttr_day'
	  ,COALESCE(rfc.rfc_score,0) 'rfc_score'
	
	FROM (
	SELECT  
	
	  sc.name_en 'supplier_name'
	  ,sc.id_supplier
	  ,(AVG(DATEDIFF(HOUR,si.sourcing_at,si.crossdocking_po_ordered_at)) - 48)/24.000 'avg_ttr_day'
	  
	  FROM StagingDB_Replica.Gathering.tblSalesItem si
	
	  LEFT JOIN StagingDB_Replica.Gathering.tblSupplierCatalog sc
	  ON sc.id_supplier = si.fk_supplier
	
	  WHERE si.sourcing_at IS NOT NULL
	  AND si.crossdocking_po_ordered_at IS NOT NULL
	
	  AND MONTH(si.created_at) = CASE WHEN MONTH(GETDATE()) = 1 THEN 12 ELSE MONTH(GETDATE())-1 END
		AND YEAR(si.created_at) = CASE WHEN MONTH(GETDATE()) = 1 THEN YEAR(GETDATE())-1 ELSE YEAR(GETDATE()) END
	
	  GROUP BY   sc.name_en, sc.id_supplier) ttr
	
	  FULL OUTER JOIN
	
	  (SELECT  
	
	  sc.name_en 'supplier_name'
	  ,sc.id_supplier
	
	  ,SUM(CASE 
	
	  WHEN si.return_reason IN (
	 'Fake A'
	,'Fake B'
	,'Fake C'
	,'Fake D'
	,'Fake Product'
	,'Merchant - Defective'
	,'Merchant-Not complete product' 
	) 
	THEN 1
	
	  WHEN si.return_reason IN (
	 'merchant - wrong item'
	,'Size too big'
	,'Size too small'
	,'wrong color'
	,'Wrong Invoice'
	,'Wrong product information'
	) 
	THEN 2
	
	  ELSE 0 END
	  
	  )  / CAST(COUNT(si.return_reason) AS FLOAT) 'rfc_score'
	 
	  
	FROM StagingDB_Replica.Gathering.tblSalesItem si
	
	LEFT JOIN StagingDB_Replica.Gathering.tblSupplierCatalog sc
	  ON sc.id_supplier = si.fk_supplier
	
	  WHERE si.return_reason IN (
	  'Fake A'
	,'Fake B'
	,'Fake C'
	,'Fake D'
	,'Fake Product'
	,'Merchant - Defective'
	,'merchant - wrong item'
	,'Merchant-Not complete product'
	,'Size too big'
	,'Size too small'
	,'wrong color'
	,'Wrong Invoice'
	,'Wrong product information'
	)
	  AND MONTH(si.created_at) = CASE WHEN MONTH(GETDATE()) = 1 THEN 12 ELSE MONTH(GETDATE())-1 END
		AND YEAR(si.created_at) = CASE WHEN MONTH(GETDATE()) = 1 THEN YEAR(GETDATE())-1 ELSE YEAR(GETDATE()) END
	
	  GROUP BY sc.id_supplier, sc.name_en) rfc
	
	  ON rfc.id_supplier = ttr.id_supplier`

	// write query result to an array of supplierscorerow.SupplierScoreRow, this array of rows represents ttrRfcTable
	var yearMonth, supplierName, iDSupplier string
	var avgTtrDay, rfcScore float32
	var ttrRfcTable []supplierscorerow.SupplierScoreRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&yearMonth, &supplierName, &iDSupplier, &avgTtrDay, &rfcScore)
		checkError(err)
		ttrRfcTable = append(ttrRfcTable,
			supplierscorerow.SupplierScoreRow{
				YearMonth:    yearMonth,
				SupplierName: supplierName,
				IDSupplier:   iDSupplier,
				AvgTtrDay:    avgTtrDay,
				RfcScore:     rfcScore})
	}

	return ttrRfcTable
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
