package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"gitlab.com/sh-migration/category/models"
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

func main()  {

	cli := GetRedisCli()

	dir := "/home/hoods/go/src/gitlab.com/sh-migration/category/xl-data/categories.csv"

	rows := ReadCSV(dir)

	var primary models.Primary
	primary.Type = "1st Tier"
	for idx , row := range rows{
		if !Exists(row[0],primary.Categories){
			primary.Categories = append(primary.Categories , models.CodeSystem{
				Code:strconv.Itoa(idx),
				Display:row[0],
			})
		}
	}
	//fmt.Println(primary)
	// Push 1st Tier Categories to REDIS
	key := fmt.Sprintf("categories")
	err := PushToRedis(cli,key,primary)
	if err != nil{
		panic(err)
	}

	for _ , category := range primary.Categories{
		fmt.Println(category)

		var secondary models.Secondary
		secondary.Type = "2nd Tier"
		for idx , row := range rows{
			if row[0]==category.Display{
				if !Exists(row[1],secondary.Categories){
					secondary.Categories = append(secondary.Categories , models.CodeSystem{
						Code:strconv.Itoa(idx),
						Display:row[1],
					})
				}
			}
		}

		fmt.Println(secondary)
		key := fmt.Sprintf("category::%s",category.Code)
		err := PushToRedis(cli,key,secondary)
		if err != nil{
			panic(err)
		}


		for _ , sub := range secondary.Categories{
			var tartiary models.Tartiary
			tartiary.Type = "3rd Tier"
			for idx , row := range rows{
				if category.Display==row[0] && sub.Display==row[1] {
					if !Exists(row[2],tartiary.Categories){
						tartiary.Categories = append(tartiary.Categories,models.CodeSystem{
							Code:strconv.Itoa(idx),
							Display:row[2],
						})
					}
				}
			}
			key := fmt.Sprintf("category::%s::sub::%s",category.Code,sub.Code)
			err := PushToRedis(cli,key,tartiary)
			if err != nil{
				panic(err)
			}
			fmt.Println(tartiary)
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