package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "aibox4you:Q43RsPAMPdTqpWfv@tcp(rm-bp1jat7yaz2y69zn26o.mysql.rds.aliyuncs.com:3306)/mqx_dev?charset=utf8mb4&parseTime=True&loc=Asia%2FShanghai")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	waitGroup := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			newFunction(db)
		}()
	}
	waitGroup.Wait()
}

func newFunction(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT INTO `mqx_messages_test-topic` (`offset`, `partition`, `message_id`, `tag`, `key`, `born_time`) SELECT COALESCE(MAX(`offset`), 0) + 1 , 1, ?, ?, ?, ? FROM `mqx_messages_test-topic` WHERE `partition` = ? ", "123", "test-topic", "123", time.Now(), 1)

	if err != nil {
		panic(err)
	}

	fmt.Printf("inserted\n")
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}
