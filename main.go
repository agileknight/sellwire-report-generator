package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	SELLWIRE_TRANSACTION_COLUMN_STATUS=16
	SELLWIRE_TRANSACTION_COLUMN_TRANSACTION_ID=2
	SELLWIRE_TRANSACTION_COLUMN_TIMESTAMP=12
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_NAME=3
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_AMOUNT=7
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_IS_EU=9
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_COUNTRY_CODE=10
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX_NUMBER=11
)

type SellwireTransaction struct {
	TransactionId string
	Date string
	CustomerName string
	Amount string
	IsEU string
	CountryCode string
	TaxNumber string
}

func main() {
	file, err := os.Open("input/SellwireOrders_All_23_Feb_2016_30_Sep_2016.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(file)

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var paypalTransactions []SellwireTransaction

	for _, record := range records {
		status := record[SELLWIRE_TRANSACTION_COLUMN_STATUS]
		if status != "complete" {
			// TODO do we need to handle refunded?
			continue
		}
		timestamp := record[SELLWIRE_TRANSACTION_COLUMN_TIMESTAMP]
		timestampParts := strings.Split(timestamp, " ")
		sellwireRecord := SellwireTransaction{
			TransactionId: record[SELLWIRE_TRANSACTION_COLUMN_TRANSACTION_ID],
			Date: timestampParts[0],
			CustomerName: record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_NAME],
			Amount: record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_AMOUNT],
			IsEU: record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_IS_EU],
			CountryCode: record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_COUNTRY_CODE],
			TaxNumber: record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX_NUMBER],
		}

		if !strings.HasPrefix(sellwireRecord.TransactionId, "ch_") {
			paypalTransactions = append(paypalTransactions, sellwireRecord)
			fmt.Print(sellwireRecord)
		}
	}
}