package main

import (
	"bdatgeo-rest-api/api"
	"errors"
	"flag"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"os"
)

func main() {
	port := flag.String("p", "80", "Port of the webserver")
	host := flag.String("h", "localhost", "Host address of the webserver")
	flag.Parse()

	address := *host + ":" + *port

	fmt.Println("Load Dataset")
	dataDirectory := os.Getenv("BDATG_REST_API_PATH")
	if dataDirectory == "" {
		panic(errors.New("no data directory specified"))
	}
	dataStore := api.LoadDataStore(dataDirectory)

	router := httprouter.New()

	router.GET("/index", dataStore.Index)
	router.GET("/all_times/:cell_id/:scenario/:var", dataStore.AllTimes)
	router.GET("/all_locations/values/:scenario/:var/:timerange", dataStore.AllLocationsValues)
	router.GET("/all_locations/grid/:scenario/:var/:timerange", dataStore.AllLocationsGrid)

	router.HandleOPTIONS = true
	router.GlobalOPTIONS = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		if header.Get("Access-Control-Request-Method") != "" {
			header.Set("Access-Control-Allow-Methods", header.Get("Allow"))
			header.Set("Access-Control-Allow-Origin", "*")
		}
		w.WriteHeader(http.StatusNoContent)
	})

	fmt.Println("Ready to Serve")
	if err := http.ListenAndServe(address, router); err != nil {
		fmt.Println(err.Error())
	}
}
