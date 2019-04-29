package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"gitlab.com/sh-migration/attribute/models"
	"os"
	"strconv"
)

func GetRedisCli() *redis.Client {

	// Redis Connection
	cli := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", "35.154.240.59", "6379"),
		Password: "shophobe247",
	})

	ping, err := cli.Ping().Result()
	if err != nil {
		fmt.Println(fmt.Sprintf("Redis Service is Offline :  %s \n", err.Error()))
	}
	fmt.Println(ping)

	return cli
}

func main() {

	cli := GetRedisCli()

	dir := "/home/hoods/go/src/gitlab.com/sh-migration/attribute/xl-data/attribute.csv"

	rows := ReadCSV(dir)

	var keys []models.CodeSystem
	for idx, row := range rows {
		if !Exists(row[0], keys) {
			keys = append(keys, models.CodeSystem{
				Code:    strconv.Itoa(idx),
				Display: row[0],
			})
		}
	}

	// Push all the keys To redis
	key := fmt.Sprintf("attributes")
	err := PushToRedis(cli, key, keys)
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		fmt.Println(key)

		var values []models.CodeSystem
		for idx, row := range rows {
			if key.Display == row[0] {
				values = append(values, models.CodeSystem{
					Code:    strconv.Itoa(idx + 1),
					Display: row[1],
				})
			}
		}

		key := fmt.Sprintf("attribute::%s", key.Code)
		err := PushToRedis(cli, key, values)
		if err != nil {
			panic(err)
		}

		fmt.Println("\t", values)
	}
}

func Exists(data string, arr []models.CodeSystem) bool {
	for _, val := range arr {
		if val.Display == data {
			return true
			break
		}
	}
	return false
}

func ReadCSV(dir string) [][]string {

	f, err := os.Open(dir)
	if err != nil {
		panic(err)
	}

	reader := csv.NewReader(bufio.NewReader(f))
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	return rows[1:]
}

func PushToRedis(cli *redis.Client, key string, value interface{}) error {
	// Marshalling the data before Pushing into Redis
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	status := cli.Set(key, string(b), 0)
	if status.Err() != nil {
		return (status.Err())
	}
	return nil
}
