package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"runtime"
	"sync"
)

const postgresConnectionInfo = "dbname=test user=test password=test sslmode=disable"

var (
	db, innerDB                     *sql.DB
	screenNameQuery, selectAllQuery *sql.Stmt
	wg                              sync.WaitGroup
)

func queryScreenName(accountId int, screenName string) {
	var dupAccountId int

	log.Printf("Account#%d: %s\n", accountId, screenName)

	dup_rows, err := screenNameQuery.Query(screenName)
	if err != nil {
		log.Fatalln(err)
	}

	for dup_rows.Next() {
		if err := dup_rows.Scan(&dupAccountId); err != nil {
			log.Fatalln(err)
		}

		log.Printf("Duplicate: %d\n", dupAccountId)
	}

	if err := dup_rows.Err(); err != nil {
		log.Fatal(err)
	}

	wg.Done()
}

func init() {
	var err error

	runtime.GOMAXPROCS(runtime.NumCPU())

	if db, err = sql.Open("postgres", postgresConnectionInfo); err != nil {
		log.Fatalln(err)
	}
	db.SetMaxOpenConns(5)

	if selectAllQuery, err = db.Prepare("SELECT id, screen_name FROM accounts"); err != nil {
		log.Fatalln(err)
	}

	if innerDB, err = sql.Open("postgres", postgresConnectionInfo); err != nil {
		log.Fatalln(err)
	}
	innerDB.SetMaxOpenConns(5)

	if screenNameQuery, err = innerDB.Prepare("SELECT id FROM accounts WHERE screen_name = $1"); err != nil {
		log.Fatalln(err)
	}
}

func main() {
	var (
		account_id  int
		screen_name string
	)

	defer db.Close()
	defer innerDB.Close()

	rows, err := selectAllQuery.Query()
	if err != nil {
		log.Fatalln(err)
	}

	for rows.Next() {
		if err := rows.Scan(&account_id, &screen_name); err != nil {
			log.Fatalln(err)
		}

		wg.Add(1)
		go queryScreenName(account_id, screen_name)
	}

	if err := rows.Err(); err != nil {
		log.Fatalln(err)
	}

	wg.Wait()
}
