package createvalidation

import (
	"database/sql"
	"log"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/thomas-bamilo/dbconf"
)

// IDSupplierRow represents a row of the table iDSupplierTable which records: "supplier_id" and "supplier_name" of each supplier which sold something in the past 3 months
type IDSupplierRow struct {
	IDSupplier   string `json:"supplier_id"`
	SupplierName string `json:"supplier_name"`
}

// BrandRow represents a row of the table brandTable which records all the SKUs sold in the past 2 months
type BrandRow struct {
	Brand string `json:"brand"`
}

// ConnectToOms returns a MySQL database connection to oms_live_ir
func ConnectToOms() *sql.DB {

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

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}

// ConnectToBob returns a MySQL database connection to bob_live_ir
func ConnectToBob() *sql.DB {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := dbConf.BobUser + ":" + dbConf.BobPw + "@tcp(" + dbConf.BobHost + ")/" + dbConf.BobDb

	// connect to database
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}

// QueryIDSupplierTable queries oms_database to create an array of IDSupplierRow which represents IDSupplierTable, the table which records:
// "id_supplier" and "supplier_name" of each supplier which sold something in the past 3 months
func QueryIDSupplierTable(db *sql.DB) []IDSupplierRow {
	// store iDSupplierQuery in a string
	iDSupplierQuery := `SELECT
  
	is1.id_supplier
	,is1.name_en 'supplier_name'
	
	FROM ims_sales_order_item isoi
	LEFT JOIN ims_supplier is1
	ON isoi.bob_id_supplier = is1.bob_id_supplier
  
	WHERE isoi.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
  
	GROUP BY is1.id_supplier, is1.name_en`

	// write iDSupplierQuery result to an array of IDSupplierRow, this array of rows represents IDSupplierTable
	var iDSupplier, supplierName string
	var iDSupplierTable []IDSupplierRow

	rows, _ := db.Query(iDSupplierQuery)

	for rows.Next() {
		err := rows.Scan(&iDSupplier, &supplierName)
		if err != nil {
			log.Fatal(err)
		}
		iDSupplierTable = append(iDSupplierTable,
			IDSupplierRow{
				IDSupplier:   iDSupplier,
				SupplierName: supplierName})
	}

	return iDSupplierTable
}

// QueryBrandTable queries all the brands which have been ordered in the past 2 months
func QueryBrandTable(db *sql.DB) []BrandRow {
	// store brandQuery in a string
	brandQuery := `SELECT DISTINCT
  
		cb.name_en
	
		FROM sales_order_item soi
		LEFT JOIN catalog_simple cs
		ON soi.sku = cs.sku
		LEFT JOIN catalog_config cc
	  	ON cc.id_catalog_config = cs.fk_catalog_config
		LEFT JOIN catalog_brand cb
		ON cb.id_catalog_brand = cc.fk_catalog_brand
		
		WHERE soi.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)`

	// write brandQuery result to an array of BrandRow, this array of rows represents brandTable
	var brand string
	var brandTable []BrandRow

	rows, _ := db.Query(brandQuery)

	for rows.Next() {
		err := rows.Scan(&brand)
		if err != nil {
			log.Fatal(err)
		}
		brandTable = append(brandTable,
			BrandRow{
				Brand: brand})
	}

	return brandTable
}
