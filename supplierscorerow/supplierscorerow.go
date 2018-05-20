package supplierscorerow

// SupplierScoreRow represents a row which records:
// id_supplier, inbound_score for each supplier
type SupplierScoreRow struct {
	YearMonth     string  `json:"year_month"`
	SupplierName  string  `json:"supplier_name"`
	IDSupplier    string  `json:"id_supplier"`
	AvgTtrDay     float32 `json:"avg_ttr_day"`
	RfcScore      float32 `json:"rfc_score"`
	InboundScore  float32 `json:"inbound_score"`
	RtsScore      float32 `json:"rts_score"`
	FinalScore    float32 `json:"final_score"`
	NetOrder      int     `json:"net_order"`
	SupplierClass string  `json:"supplier_class"`
}
