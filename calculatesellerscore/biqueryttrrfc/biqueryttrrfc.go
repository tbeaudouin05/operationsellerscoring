package biqueryttrrfc

import (
	"database/sql"
	"log"

	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thomas-bamilo/dbconf"
)

// TtrRfcRow represents a row of the table which records: "time to respond" and "return from customer score" for each seller on a year_month basis
type TtrRfcRow struct {
	YearMonth    string `json:"year_month"`
	SupplierName string `json:"supplier_name"`
	IDSupplier   string `json:"id_supplier"`
	AvgTtrDay    string `json:"avg_ttr_day"`
	RfcScore     string `json:"rfc_score"`
}

// CreateTtrRfcTable queries bi_database to create an array of TtrRfcRow which represent TtrRfcTable, the table which records: "time to respond" and "return from customer score" for each seller on a year_month basis
func CreateTtrRfcTable() []TtrRfcRow {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := `sqlserver://` + dbConf.BiUser + ":" + dbConf.BiPw + "@" + dbConf.BiHost + "/" + dbConf.BiDb

	// connect to database
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	// store the query in a string
	query := `
	SELECT
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
	  
	  FROM tblSalesItem si
	
	  LEFT JOIN tblSupplierCatalog sc
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
	 
	  
	FROM tblSalesItem si
	
	LEFT JOIN tblSupplierCatalog sc
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

	// write query result to an array of TtrRfcRow, this array of rows represents ttrRfcTable
	var yearMonth, supplierName, iDSupplier, avgTtrDay, rfcScore string
	var ttrRfcTable []TtrRfcRow

	rows, _ := db.Query(query)

	for rows.Next() {
		err := rows.Scan(&yearMonth, &supplierName, &iDSupplier, &avgTtrDay, &rfcScore)
		if err != nil {
			log.Fatal(err)
		}
		ttrRfcTable = append(ttrRfcTable,
			TtrRfcRow{
				YearMonth:    yearMonth,
				SupplierName: supplierName,
				IDSupplier:   iDSupplier,
				AvgTtrDay:    avgTtrDay,
				RfcScore:     rfcScore})
	}

	return ttrRfcTable
}
