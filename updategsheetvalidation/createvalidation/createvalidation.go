package createvalidation

import (
	"database/sql"
	"log"
	"time"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	spreadsheet "gopkg.in/Iwark/spreadsheet.v2"
)


// IDSupplierRow represents a row of the table iDSupplierTable which records: "supplier_id" and "supplier_name" of each supplier which sold something in the past 3 months
type IDSupplierRow struct {
	IDSupplier   string `json:"supplier_id"`
	SupplierName string `json:"supplier_name"`
}


// EmailRow represents a row of the table userTable which records all the OMS users' emails and id_user
type EmailRow struct {
	Email   string `json:"email"`
	IDEmail string `json:"id_email"`
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
		checkError(err)
		iDSupplierTable = append(iDSupplierTable,
			IDSupplierRow{
				IDSupplier:   iDSupplier,
				SupplierName: supplierName})
	}

	return iDSupplierTable
}

// EmailToBaa fetches all the emails from the sheet: https://goo.gl/o26ubW, uses uniqueEmail function to only keep unique emails and upload it to baa database
func EmailToBaa(spreadsheet spreadsheet.Spreadsheet, dbBaa *sql.DB) {

	gsheet, err := spreadsheet.SheetByID(199289760)
	checkError(err)

	var emailTable []EmailRow

	for _, row := range gsheet.Rows[1:] {

		emailTable = append(emailTable,
			EmailRow{
				Email: row[7].Value,
			})
	}

	uniqueEmailTable := uniqueEmail(emailTable)

	oldEmailTable := BaaToEmailTable(dbBaa)

	newEmailTable := newEmail(oldEmailTable, uniqueEmailTable)

	// prepare statement to insert values into inbound_issue table
	insertEmailTableStr := `INSERT INTO baa_application.baa_application_schema.inbound_issue_email (email) 
	VALUES (@p1)`
	insertEmailTable, err := dbBaa.Prepare(insertEmailTableStr)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(newEmailTable); i++ {
		_, err = insertEmailTable.Exec(
			newEmailTable[i].Email,
		)
		if err != nil {

			log.Println(err.Error())
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// takes an array of EmailRow and returns an array of unique EmailRow
func uniqueEmail(emailTable []EmailRow) []EmailRow {
	uniqueEmailTable := make([]EmailRow, 0, len(emailTable))
	uniqueEmailMap := make(map[EmailRow]bool)

	for _, emailRow := range emailTable {
		if _, ok := uniqueEmailMap[emailRow]; !ok {
			uniqueEmailMap[emailRow] = true
			uniqueEmailTable = append(uniqueEmailTable, emailRow)
		}
	}

	return uniqueEmailTable
}

// takes two arrays of UNIQUE EmailRow and only returns non-matching EmailRow ie only returns new EmailRow into an array of EmailRow: newEmailTable
func newEmail(oldEmailTable, emailTable []EmailRow) (newEmailTable []EmailRow) {

	// initialize oldEmailMap with oldEmailRow
	oldEmailMap := make(map[string]bool)
	for _, oldEmail := range oldEmailTable {
		oldEmailMap[oldEmail.Email] = true
	}

	// check if any EmailRow from emailTable is not in oldEmailMap - if not, add the EmailRow to newEmailTable
	for _, emailRow := range emailTable {
		if _, ok := oldEmailMap[emailRow.Email]; !ok {
			newEmailTable = append(newEmailTable, emailRow)
		}

	}
	return newEmailTable
}

// BaaToEmailTable queries baa_application.inbound_issue_email and store the table into an array of EmailRow
func BaaToEmailTable(dbBaa *sql.DB) []EmailRow {

	// store the query in a string
	query := `SELECT iie.email, iie.id_email FROM baa_application.baa_application_schema.inbound_issue_email iie`

	var email, iDEmail string
	var emailTable []EmailRow

	rows, err := dbBaa.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&email, &iDEmail)
		checkError(err)
		emailTable = append(emailTable,
			EmailRow{
				Email:   email,
				IDEmail: iDEmail,
			})
	}

	return emailTable

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
