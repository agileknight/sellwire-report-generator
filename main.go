package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"log"
	"os"
	"strings"
	"time"
)

const (
	SELLWIRE_TIMESTAMP_FORMAT = "2006-01-02 15:04:05"
	SELLWIRE_TRANSACTION_COLUMN_STATUS=16
	SELLWIRE_TRANSACTION_COLUMN_TRANSACTION_ID=2
	SELLWIRE_TRANSACTION_COLUMN_TIMESTAMP=12
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_NAME=3
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX=6
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_AMOUNT=7
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_IS_EU=9
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_COUNTRY_CODE=10
	SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX_NUMBER=11
	PAYPAL_DATE_OUTPUT_FORMAT = "02.01.2006"
	STRIPE_TRANSFER_DATE_FORMAT = "2006-01-02 15:04"
	STRIPE_TRANSFER_COLUMN_STATUS=3
	STRIPE_TRANSFER_COLUMN_DATE=0
	STRIPE_TRANSFER_COLUMN_TRANSFER_ID=1
	STRIPE_TRANSFER_COLUMND_AMOUNT=5
	STRIPE_PAYMENT_COLUMN_PAYMENT_ID=0
	STRIPE_PAYMENT_COLUMN_TRANSFER_ID=45
	STRIPE_PAYMENT_COLUMN_STATUS=12
)

type TransactionType string

const (
	TransactionTypePaypal TransactionType = "paypal"
	TransactionTypeStripe TransactionType = "stripe"
)

type Amount struct {
	Dollars int64
	Cents int64
}

func (a Amount) ToStringGermany() string {
	if a.Dollars == 0 && a.Cents == 0 {
		return ""
	}
	return fmt.Sprintf("%d,%02d",a.Dollars, a.Cents)
}

// VAT % of total amount (including VAT)
func (a Amount) VATPercentOfAsStringGermany(other Amount) string {
	totalCentsA := a.Dollars * 100 + a.Cents
	totalCentsB := other.Dollars * 100 + other.Cents - totalCentsA
	percentage := float64(totalCentsA) * 100.0 / float64(totalCentsB)
	percentageRounded := int64(percentage + 0.5)
	if percentageRounded == 0 {
		return ""
	}
	return fmt.Sprintf("%d%%", percentageRounded)
}

type SellwireTransaction struct {
	TransactionType TransactionType
	TransactionId string
	Timestamp time.Time
	CustomerName string
	Amount Amount
	TaxAmount Amount
	IsEU bool
	IsPrivate bool
	CountryCode string
	TaxNumber string
}

type StripeTransfer struct {
	TransferId string
	Date time.Time
	Amount Amount
	Status string
}

var transactions []SellwireTransaction
var stripeTransfersByTransactionId map[string]StripeTransfer

func main() {
	importSellwireTransactions()
	importStripeTransferMap()
	outputPaypalTransactions()
	outputStripeTransactions()
}

func importSellwireTransactions() {
	sellwireOrdersFile, err := os.Open("input/SellwireOrders_All_23_Feb_2016_30_Sep_2016.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(sellwireOrdersFile)

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, record := range records[1:] {
		status := record[SELLWIRE_TRANSACTION_COLUMN_STATUS]
		if status != "complete" {
			continue
		}
		timestampStr := record[SELLWIRE_TRANSACTION_COLUMN_TIMESTAMP]
		timestamp, err := time.Parse(SELLWIRE_TIMESTAMP_FORMAT, timestampStr)
		if err != nil {
			log.Fatal(err)
		}

		countryCode := record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_COUNTRY_CODE]
		if countryCode == "" {
			countryCode = "US"
		}
		taxNumber := strings.ToUpper(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX_NUMBER])

		if taxNumber != "" && !strings.HasPrefix(taxNumber, countryCode) {
			taxNumber = fmt.Sprintf("%s%s", countryCode, taxNumber)
		}

		isEU, _ := strconv.ParseBool(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_IS_EU])

		amount := parseAmount(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_AMOUNT])
		taxAmount := parseAmount(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_TAX])

		transactionId := record[SELLWIRE_TRANSACTION_COLUMN_TRANSACTION_ID]

		transactionType := TransactionTypePaypal
		if strings.HasPrefix(transactionId, "ch_") {
			transactionType = TransactionTypeStripe
		}

		sellwireRecord := SellwireTransaction{
			TransactionType: transactionType,
			TransactionId: transactionId,
			Timestamp: timestamp,
			CustomerName: strings.Title(strings.ToLower(record[SELLWIRE_TRANSACTION_COLUMN_CUSTOMER_NAME])),
			Amount: amount,
			TaxAmount: taxAmount,
			IsEU: isEU,
			IsPrivate: isEU && taxNumber == "",
			CountryCode: countryCode,
			TaxNumber: taxNumber,
		}

		transactions = append(transactions, sellwireRecord)
	}
}

func parseAmount(amountStr string) Amount {
			var amount Amount
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
					if cents < 10 { // 9.8 is 9 dollar 80 cents and not 9 dollar 8 cents
						cents *= 10
					}
					if err != nil {
						log.Fatal(err)
					}
					amount.Cents = cents
				}
			}
			return amount
		}

func importStripeTransferMap() {
	stripeTransfersByTransferId := make(map[string]StripeTransfer)

	stripeTransfersFile, err := os.Open("input/transfers.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(stripeTransfersFile)

	transferRecords, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, record := range transferRecords[1:] {
		status := record[STRIPE_TRANSFER_COLUMN_STATUS]

		dateStr := record[STRIPE_TRANSFER_COLUMN_DATE]
		date, err := time.Parse(STRIPE_TRANSFER_DATE_FORMAT, dateStr)
		if err != nil {
			log.Fatal(err)
		}

		var amount Amount
		amountStr := record[STRIPE_TRANSFER_COLUMND_AMOUNT]
		amountStrStripped := strings.Replace(amountStr, ".", "", -1)
		amountParts := strings.Split(amountStrStripped, ",")
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

		transferId := record[STRIPE_TRANSFER_COLUMN_TRANSFER_ID]

		transferRecord := StripeTransfer{
			TransferId: transferId,
			Date: date,
			Amount: amount,
			Status: status,
		}

		stripeTransfersByTransferId[transferId] = transferRecord
	}

	stripePaymentsFile, err := os.Open("input/payments.csv")
	if err != nil {
		log.Fatal(err)
	}
	r2 := csv.NewReader(stripePaymentsFile)

	paymentRecords, err := r2.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	stripeTransfersByTransactionId = make(map[string]StripeTransfer)

	for _, record := range paymentRecords[1:] {
		paymentId := record[STRIPE_PAYMENT_COLUMN_PAYMENT_ID]
		transferId := record[STRIPE_PAYMENT_COLUMN_TRANSFER_ID]
		status := record[STRIPE_PAYMENT_COLUMN_STATUS]

		if status != "Paid" {
			continue
		}

		transfer, ok := stripeTransfersByTransferId[transferId]
		if !ok {
			log.Fatalf("transfer id %s no transfer found for payment id %s", transferId, paymentId)
		}
		stripeTransfersByTransactionId[paymentId] = transfer
	}
}

func outputPaypalTransactions() {
	paypalOutput := [][]string{
		{"Datum", "Kundenname", "Betrag USD", "VAT USD", "VAT", "Land", "EU", "Privat", "USt-ID"},
	}

	for _, tx := range transactions {
		if tx.TransactionType != TransactionTypePaypal {
			continue
		}

		isEU := ""
		if tx.IsEU {
			isEU = "x"
		}

		isPrivate := ""
		if tx.IsPrivate {
			isPrivate = "x"
		}
		if !tx.IsEU {
			isPrivate = "-"
		}

		record := []string{
			tx.Timestamp.Format(PAYPAL_DATE_OUTPUT_FORMAT),
			tx.CustomerName,
			tx.Amount.ToStringGermany(),
			tx.TaxAmount.ToStringGermany(),
			tx.TaxAmount.VATPercentOfAsStringGermany(tx.Amount),
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
		log.Fatalln("error writing paypal output csv: %v", err)
	}
}

func outputStripeTransactions() {
	stripeOutput := [][]string{
		{"Datum", "Kundenname", "Betrag USD", "VAT USD", "VAT", "Land", "EU", "Privat", "USt-ID", "Datum Transfer", "Gesamtbetrag Transfer EUR"},
	}

	for _, tx := range transactions {
		if tx.TransactionType != TransactionTypeStripe {
			continue
		}

		isEU := ""
		if tx.IsEU {
			isEU = "x"
		}

		isPrivate := ""
		if tx.IsPrivate {
			isPrivate = "x"
		}
		if !tx.IsEU {
			isPrivate = "-"
		}

		transfer := stripeTransfersByTransactionId[tx.TransactionId]

		record := []string{
			tx.Timestamp.Format(PAYPAL_DATE_OUTPUT_FORMAT),
			tx.CustomerName,
			tx.Amount.ToStringGermany(),
			tx.TaxAmount.ToStringGermany(),
			tx.TaxAmount.VATPercentOfAsStringGermany(tx.Amount),
			tx.CountryCode,
			isEU,
			isPrivate,
			tx.TaxNumber,
			transfer.Date.Format(PAYPAL_DATE_OUTPUT_FORMAT),
			transfer.Amount.ToStringGermany(),
		}
		stripeOutput = append(stripeOutput, record)
	}

	outputFile, err := os.Create("output/Stripe.csv")
	if err != nil {
		log.Fatal(err)
	}

	w := csv.NewWriter(outputFile)
	w.WriteAll(stripeOutput)

	if err := w.Error(); err != nil {
		log.Fatalln("error writing stripe output csv: %v", err)
	}
}
