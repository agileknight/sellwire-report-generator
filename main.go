package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"encoding/json"
	"net/http"
)

const (
	EDD_TIMESTAMP_FORMAT                     = "2006-01-02 15:04:05"
	EDD_PAYMENT_COLUMN_PAYMENT_METHOD        = 17
	EDD_PAYMENT_COLUMN_STATUS                = 25
	EDD_PAYMENT_COLUMN_TIMESTAMP             = 20
	EDD_PAYMENT_COLUMN_CUSTOMER_EMAIL        = 2
	EDD_PAYMENT_COLUMN_CUSTOMER_FIRST_NAME   = 4
	EDD_PAYMENT_COLUMN_CUSTOMER_LAST_NAME    = 5
	EDD_PAYMENT_COLUMN_CUSTOMER_TAX          = 15
	EDD_PAYMENT_COLUMN_CUSTOMER_AMOUNT       = 14
	EDD_PAYMENT_COLUMN_CUSTOMER_COUNTRY_CODE = 10
	EDD_PAYMENT_COLUMN_CUSTOMER_TAX_NUMBER   = 28
	EDD_PAYMENT_COLUMN_VAT_RATE              = 30
	EDD_PAYMENT_COLUMN_IP_ADDRESS            = 27

	STRIPE_DATE_FORMAT                        = "2006-01-02 15:04"

	// payouts.csv
	STRIPE_TRANSFER_COLUMN_STATUS             = "Status"
	STRIPE_TRANSFER_COLUMN_DATE               = "Arrival Date (UTC)"
	STRIPE_TRANSFER_COLUMN_TRANSFER_ID        = "id"
	STRIPE_TRANSFER_COLUMND_AMOUNT            = "Amount"

	// payments.csv
	STRIPE_PAYMENT_COLUMN_PAYMENT_ID          = "id"
	STRIPE_PAYMENT_COLUMN_PAYMENT_CUSTOMER_ID = "Customer Email"
	STRIPE_PAYMENT_COLUMN_PAYMENT_DATE        = "Created (UTC)"
	STRIPE_PAYMENT_COLUMN_TRANSFER_ID         = "Transfer"
	STRIPE_PAYMENT_COLUMN_STATUS              = "Status"

	REPORT_DATE_OUTPUT_FORMAT = "02.01.2006"
)

type Amount struct {
	Dollars int64
	Cents   int64
}

func (a Amount) IsZero() bool {
	return a.Dollars == 0 && a.Cents == 0
}

func (a Amount) ToStringGermany() string {
	if a.Dollars == 0 && a.Cents == 0 {
		return ""
	}
	return fmt.Sprintf("%d,%02d", a.Dollars, a.Cents)
}

// VAT % of total amount (including VAT)
func (a Amount) VATPercentOfAsStringGermany(other Amount) string {
	totalCentsA := a.Dollars*100 + a.Cents
	totalCentsB := other.Dollars*100 + other.Cents - totalCentsA
	percentage := float64(totalCentsA) * 100.0 / float64(totalCentsB)
	percentageRounded := int64(percentage + 0.5)
	if percentageRounded == 0 {
		return ""
	}
	return fmt.Sprintf("%d%%", percentageRounded)
}

type EddPayment struct {
	PaymentMethod string
	Timestamp     time.Time
	CustomerEmail string
	CustomerName  string
	Amount        Amount
	TaxAmount     Amount
	IsEU          bool
	IsPrivate     bool
	CountryCode   string
	TaxNumber     string
	IsRefund      bool
}

type StripeTransfer struct {
	TransferId string
	Date       time.Time
	Amount     Amount
	Status     string
}

type GeoIpInfo struct {
	CountryCode               string   `json:"country"`
}

var payments []EddPayment
var stripeTransfersByTransactionKey map[string]StripeTransfer

func main() {
	importEddPayments()
	importStripeTransferMap()

	var limitMonth int64
	var limitYear int64
	var err error
	if len(os.Args) == 2 {
		limitYear, err = strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil {
			panic(err)
		}
		log.Printf("Limiting output to year %d", limitYear)
	}
	if len(os.Args) == 3 {
		limitMonth, err = strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil {
			panic(err)
		}
		limitYear, err = strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			panic(err)
		}
		log.Printf("Limiting output to month and year %d %d", limitMonth, limitYear)
	}
	outputStripeTransactions(int(limitMonth), int(limitYear))
	outputPaypalTransactions(int(limitMonth), int(limitYear))
}

func resolveIpAddressToCountryCode(ipAddress, customerEmail string, timestamp time.Time) string {
	url := fmt.Sprintf("https://ipinfo.io/%s/json", ipAddress)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("NewRequest: ", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Do: ", err)
	}
	defer resp.Body.Close()

	var record GeoIpInfo
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		log.Fatal("Decode: ", err)
	}

	log.Printf("Looked up country code %v from IP address: %s for transaction %v %v \n", record.CountryCode, ipAddress, customerEmail, timestamp)
	return record.CountryCode
}

func importEddPayments() {
	eddPaymentsFile, err := os.Open("input/edd-export-payments.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(eddPaymentsFile)

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, record := range records[1:] {
		paymentMethod := record[EDD_PAYMENT_COLUMN_PAYMENT_METHOD]

		status := record[EDD_PAYMENT_COLUMN_STATUS]
		if status != "complete" && status != "refunded" && status != "edd_subscription" {
			continue
		}

		timestampStr := record[EDD_PAYMENT_COLUMN_TIMESTAMP]
		timestamp, err := time.Parse(EDD_TIMESTAMP_FORMAT, timestampStr)
		if err != nil {
			log.Fatal(err)
		}

		amount := parseAmount(record[EDD_PAYMENT_COLUMN_CUSTOMER_AMOUNT])
		taxAmount := parseAmount(record[EDD_PAYMENT_COLUMN_CUSTOMER_TAX])

		customerEmail := record[EDD_PAYMENT_COLUMN_CUSTOMER_EMAIL]

		countryCode := record[EDD_PAYMENT_COLUMN_CUSTOMER_COUNTRY_CODE]
		taxNumber := strings.ToUpper(record[EDD_PAYMENT_COLUMN_CUSTOMER_TAX_NUMBER])

		isEU := !taxAmount.IsZero() || taxNumber != ""

		// special handling of ip-only-vat
		if record[EDD_PAYMENT_COLUMN_VAT_RATE] == "??" && !taxAmount.IsZero() {
			ipAddress := record[EDD_PAYMENT_COLUMN_IP_ADDRESS]
			lookedUpCountryCode := resolveIpAddressToCountryCode(ipAddress, customerEmail, timestamp)
			if lookedUpCountryCode != countryCode {
				log.Printf("Changed from billing country %v to %v", countryCode, lookedUpCountryCode)
				countryCode = lookedUpCountryCode
			}
		}

		isRefund := status == "refunded"
		isPrivate := isEU && taxNumber == ""

		customerFirstName := strings.Title(strings.ToLower(record[EDD_PAYMENT_COLUMN_CUSTOMER_FIRST_NAME]))
		customerLastName := strings.Title(strings.ToLower(record[EDD_PAYMENT_COLUMN_CUSTOMER_LAST_NAME]))
		customerName := customerFirstName
		if customerLastName != "" {
			customerName = customerName + " " + customerLastName
		}

		eddPayment := EddPayment{
			PaymentMethod: paymentMethod,
			Timestamp:     timestamp,
			CustomerEmail: customerEmail,
			CustomerName:  customerName,
			Amount:        amount,
			TaxAmount:     taxAmount,
			IsEU:          isEU,
			IsPrivate:     isPrivate,
			CountryCode:   countryCode,
			TaxNumber:     taxNumber,
			IsRefund:      isRefund,
		}

		payments = append(payments, eddPayment)
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
			cents, err := strconv.ParseInt(amountParts[1], 10, 64)
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

func getColumn(columnName string, nameMapping map[string]int, columns []string ) string {
	columnIndex, ok := nameMapping[columnName]
	if !ok {
		panic("unknown column name: " + columnName)
	}
	return columns[columnIndex]
}

func importStripeTransferMap() {
	stripeTransfersByTransferId := make(map[string]StripeTransfer)

	stripeTransfersFile, err := os.Open("input/payouts.csv")
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(stripeTransfersFile)

	transferRecords, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	transferColumnIndexByName := make(map[string]int)
	for index, columnName := range transferRecords[0] {
		transferColumnIndexByName[columnName] = index
	}

	for _, record := range transferRecords[1:] {
		status := getColumn(STRIPE_TRANSFER_COLUMN_STATUS, transferColumnIndexByName, record)

		dateStr := getColumn(STRIPE_TRANSFER_COLUMN_DATE, transferColumnIndexByName, record)
		date, err := time.Parse(STRIPE_DATE_FORMAT, dateStr)
		if err != nil {
			log.Fatal(err)
		}

		var amount Amount
		amountStr := getColumn(STRIPE_TRANSFER_COLUMND_AMOUNT, transferColumnIndexByName, record)
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
				cents, err := strconv.ParseInt(amountParts[1], 10, 64)
				if err != nil {
					log.Fatal(err)
				}
				amount.Cents = cents
			}
		}

		transferId := getColumn(STRIPE_TRANSFER_COLUMN_TRANSFER_ID, transferColumnIndexByName, record)

		transferRecord := StripeTransfer{
			TransferId: transferId,
			Date:       date,
			Amount:     amount,
			Status:     status,
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

	stripeTransfersByTransactionKey = make(map[string]StripeTransfer)

	paymentColumnIndexByName := make(map[string]int)
	for index, columnName := range paymentRecords[0] {
		paymentColumnIndexByName[columnName] = index
	}

	for _, record := range paymentRecords[1:] {
		paymentId := getColumn(STRIPE_PAYMENT_COLUMN_PAYMENT_ID, paymentColumnIndexByName, record)
		customerId := getColumn(STRIPE_PAYMENT_COLUMN_PAYMENT_CUSTOMER_ID, paymentColumnIndexByName, record)
		transferId := getColumn(STRIPE_PAYMENT_COLUMN_TRANSFER_ID, paymentColumnIndexByName, record)
		status := getColumn(STRIPE_PAYMENT_COLUMN_STATUS, paymentColumnIndexByName, record)
		paymentDateStr := getColumn(STRIPE_PAYMENT_COLUMN_PAYMENT_DATE, paymentColumnIndexByName, record)
		paymentDate, err := time.Parse(STRIPE_DATE_FORMAT, paymentDateStr)
		if err != nil {
			log.Fatal(err)
		}

		if status != "Paid" && status != "Refunded" {
			continue
		}

		transfer, ok := stripeTransfersByTransferId[transferId]
		if !ok {
			log.Printf("transfer id %s no transfer found for payment id %s from date %v", transferId, paymentId, paymentDate)
		}

		transactionKey := transactionKeyFromCustomerIdAndTimestamp(customerId, paymentDate)
		stripeTransfersByTransactionKey[transactionKey] = transfer
		// we also put it in under the minute before and after that in case we exactly hit a minute boarder
		transactionKeyMinuteBefore := transactionKeyFromCustomerIdAndTimestamp(customerId, paymentDate.Add(-1*time.Minute))
		stripeTransfersByTransactionKey[transactionKeyMinuteBefore] = transfer
		transactionKeyMinuteAfter := transactionKeyFromCustomerIdAndTimestamp(customerId, paymentDate.Add(time.Minute))
		stripeTransfersByTransactionKey[transactionKeyMinuteAfter] = transfer
	}
}

// payment id can currently not be used for lookup as edd does not export it
// instead we use a combination of customer id and the timestamp rounded to the minute
func transactionKeyFromCustomerIdAndTimestamp(customerId string, timestamp time.Time) string {
	return fmt.Sprintf("%s_%d", customerId, timestamp.Truncate(time.Minute).Unix())
}

func outputStripeTransactions(limitMonth, limitYear int) {
	stripeOutput := [][]string{
		{"Datum", "Kundenname", "USD", "VAT", "%", "Land", "EU", "Privat", "USt-ID", "Transfer", "EUR", "Rückerst"},
	}

	for _, tx := range payments {
		if tx.PaymentMethod != "Stripe" {
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

		isRefund := ""
		if tx.IsRefund {
			isRefund = "x"
		}

		// use customer email as id because real id is just an internal number
		transactionKey := transactionKeyFromCustomerIdAndTimestamp(tx.CustomerEmail, tx.Timestamp)
		transfer, ok := stripeTransfersByTransactionKey[transactionKey]
		if !ok {
			log.Printf("no transfer found for payment %+v", tx)
			continue
		}

		if limitMonth > 0 && limitMonth != int(transfer.Date.Month()) {
			continue
		}
		if limitYear > 0 && limitYear != transfer.Date.Year() {
			continue
		}

		record := []string{
			tx.Timestamp.Format(REPORT_DATE_OUTPUT_FORMAT),
			tx.CustomerName,
			tx.Amount.ToStringGermany(),
			tx.TaxAmount.ToStringGermany(),
			tx.TaxAmount.VATPercentOfAsStringGermany(tx.Amount),
			tx.CountryCode,
			isEU,
			isPrivate,
			tx.TaxNumber,
			transfer.Date.Format(REPORT_DATE_OUTPUT_FORMAT),
			transfer.Amount.ToStringGermany(),
			isRefund,
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

func outputPaypalTransactions(limitMonth, limitYear int) {
	paypalOutput := [][]string{
		{"Datum", "Kundenname", "USD", "VAT", "%", "Land", "EU", "Privat", "USt-ID", "Rückerst"},
	}

	for _, tx := range payments {
		if tx.PaymentMethod != "PayPal Standard" {
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

		isRefund := ""
		if tx.IsRefund {
			isRefund = "x"
		}

		if limitMonth > 0 && limitMonth != int(tx.Timestamp.Month()) {
			continue
		}
		if limitYear > 0 && limitYear != tx.Timestamp.Year() {
			continue
		}

		record := []string{
			tx.Timestamp.Format(REPORT_DATE_OUTPUT_FORMAT),
			tx.CustomerName,
			tx.Amount.ToStringGermany(),
			tx.TaxAmount.ToStringGermany(),
			tx.TaxAmount.VATPercentOfAsStringGermany(tx.Amount),
			tx.CountryCode,
			isEU,
			isPrivate,
			tx.TaxNumber,
			isRefund,
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
