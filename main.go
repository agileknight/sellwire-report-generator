package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
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

type Amount struct {
	Dollars int64
	Cents int64
}

type SellwireTransaction struct {
	TransactionId string
	Date string
	CustomerName string
	Amount Amount
	IsEU bool
	IsPrivate bool
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

		countryCode := record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_COUNTRY_CODE]
		taxNumber := record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX_NUMBER]

		if taxNumber != "" && !strings.HasPrefix(taxNumber, countryCode) {
			taxNumber = fmt.Sprintf("%s%s", countryCode, taxNumber)
		}

		isEU, _ := strconv.ParseBool(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_IS_EU])

		var amount Amount
		amountStr := record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_AMOUNT]
		amountStrStripped := strings.Replace(amountStr, ",", "", -1)
		amountParts := strings.Split(amountStrStripped, ".")
		if len(amountParts) > 2 {
			log.Fatalf("Invalid amount found: %s", amountStr)
		}
		if len(amountParts) > 0 {
			dollars, err := strconv.ParseInt(amountParts[0], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			amount.Dollars = dollars
			if len(amountParts) > 1 {
				cents, err  := strconv.ParseInt(amountParts[1], 10, 64)
				if err != nil {
					log.Fatal(err)
				}
				amount.Cents = cents
			}
		}

		sellwireRecord := SellwireTransaction{
			TransactionId: record[SELLWIRE_TRANSACTION_COLUMN_TRANSACTION_ID],
			Date: timestampParts[0],
			CustomerName: strings.Title(strings.ToLower(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_NAME])),
			Amount: amount,
			IsEU: isEU,
			IsPrivate: taxNumber == "",
			CountryCode: countryCode,
			TaxNumber: taxNumber,
		}

		if !strings.HasPrefix(sellwireRecord.TransactionId, "ch_") {
			paypalTransactions = append(paypalTransactions, sellwireRecord)
		}
	}

	paypalOutput := [][]string{
		{"Datum", "Kundenname", "Betrag USD", "Land", "EU", "Privat", "USt-ID"},
	}

	for _, tx := range paypalTransactions {
		isEU := ""
		if tx.IsEU {
			isEU = "x"
		}

		isPrivate := ""
		if tx.IsPrivate {
			isPrivate = "x"
		}

		record := []string{
			tx.Date,
			tx.CustomerName,
			fmt.Sprintf("%d,%02d",tx.Amount.Dollars, tx.Amount.Cents),
			tx.CountryCode,
			isEU,
			isPrivate,
			tx.TaxNumber,
		}
		paypalOutput = append(paypalOutput, record)
	}

	outputFile, err := os.Create("output/Paypal.csv")
	if err != nil {
		log.Fatal(err)
	}

	w := csv.NewWriter(outputFile)
	w.WriteAll(paypalOutput)

	if err := w.Error(); err != nil {
		log.Fatalln("error writing paypal output csv:", err)
	}
}