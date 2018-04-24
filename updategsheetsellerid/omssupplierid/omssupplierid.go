package omssupplierid

import (
	"database/sql"
	"log"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/thomas-bamilo/dbconf"
)

// SupplierIDRow represents a row of the table supplierIDTable which records: "supplier_id" and "supplier_name" of each supplier which sold something in the past 3 months
type SupplierIDRow struct {
	SupplierID   string `json:"supplier_id"`
	SupplierName string `json:"supplier_name"`
}

// CreateSupplierIDTable queries oms_database to create an array of SupplierIDRow which represents supplierIDTable, the table which records: "supplier_id" and "supplier_name" of each supplier which sold something in the past 3 months
func CreateSupplierIDTable() []SupplierIDRow {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := dbConf.OmsUser + ":" + dbConf.OmsPw + "@tcp(" + dbConf.OmsHost + ")/" + dbConf.OmsDb

	// connect to database
	db, err := sql.Open("mysql", connStr)
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
	query := `SELECT
  
	is1.id_supplier
	,is1.name_en 
	
	FROM ims_sales_order_item isoi
	LEFT JOIN ims_supplier is1
	ON isoi.bob_id_supplier = is1.bob_id_supplier
  
	WHERE isoi.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
  
	GROUP BY is1.id_supplier, is1.name_en`

	// write query result to an array of SupplierIDRow, this array of rows represents supplierIDTable
	var supplierID, supplierName string
	var supplierIDTable []SupplierIDRow

	rows, _ := db.Query(query)

	for rows.Next() {
		err := rows.Scan(&supplierID, &supplierName)
		if err != nil {
			log.Fatal(err)
		}
		supplierIDTable = append(supplierIDTable,
			SupplierIDRow{
				SupplierID:   supplierID,
				SupplierName: supplierName})
	}

	return supplierIDTable
}
