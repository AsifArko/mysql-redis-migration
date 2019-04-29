package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"gitlab.com/sh-migration/location/models"
	"os"
	"strconv"
)

func main() {

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

	// Location Excel Directory
	dir := "/home/hoods/go/src/gitlab.com/sh-migration/location/xl-data/location.csv"

	// Reads the CSV and returns the rows accept the column labels
	rows := ReadCSV(dir)

	// Generate city information
	var cities []models.CodeSystem
	for idx, row := range rows {
		if !Exists(row[1], cities) {
			cities = append(cities, models.CodeSystem{
				Code:    strconv.Itoa(idx),
				Display: row[1],
			})
		}
	}

	// Push Cities Array to redis
	key := fmt.Sprintf("cities")
	// Marshalling the data before Pushing into Redis
	b, err := json.Marshal(cities)
	if err != nil {
		panic(err)
	}

	status := cli.Set(key, string(b), 0)
	if status.Err() != nil {
		panic(status.Err())
	}

	for _, city := range cities {
		fmt.Println(city)

		var areas []models.CodeSystem
		for idx, row := range rows {
			if city.Display == row[1] {
				areas = append(areas, models.CodeSystem{
					Code:    strconv.Itoa(idx),
					Display: row[3],
				})
			}
		}
		fmt.Println(areas)

		key := fmt.Sprintf("cities::%s", city.Code)
		// Marshalling the data before Pushing into Redis
		b, err := json.Marshal(areas)
		if err != nil {
			panic(err)
		}

		status := cli.Set(key, string(b), 0)
		if status.Err() != nil {
			panic(status.Err())
		}
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
