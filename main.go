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

func getDataStore() *api.DataStore {
	fmt.Println("Load Dataset")
	dataDirectory := os.Getenv("BDATG_REST_API_PATH")
	if dataDirectory == "" {
		panic(errors.New("no data directory specified"))
	}
	dataStore := api.LoadDataStore(dataDirectory)
	return &dataStore
}

func getFieldClimateAPI() *api.FieldClimateAPI {
	// privateKey := os.Getenv("FIELDCLIMATE_URL")
	// if privateKey == "" {
	// 	panic(errors.New("no field climate api url found"))
	// }

	privateKey := os.Getenv("FIELD_CLIMATE_PRIVATE_KEY")
	if privateKey == "" {
		panic(errors.New("no field climate private key found"))
	}

	publicKey := os.Getenv("FIELD_CLIMATE_PUBLIC_KEY")
	if publicKey == "" {
		panic(errors.New("no field climate public key found"))
	}

	fieldClimateAPI := api.NewFieldClimateAPI(publicKey, privateKey, "https://api.fieldclimate.com/v1")
	return &fieldClimateAPI
}

func main() {
	port := flag.String("p", "80", "Port of the webserver")
	host := flag.String("h", "localhost", "Host address of the webserver")
	flag.Parse()

	address := *host + ":" + *port

	dataStore := getDataStore()
	fieldClimateAPI := getFieldClimateAPI()

	router := httprouter.New()

	router.GET("/index", dataStore.Index)
	router.GET("/all_times/:cell_id/:scenario/:var", dataStore.AllTimes)
	router.GET("/all_locations/values/:scenario/:var/:timerange", dataStore.AllLocationsValues)
	router.GET("/all_locations/grid/:scenario/:var/:timerange", dataStore.AllLocationsGrid)

	router.GET("/fieldclimate/sources", fieldClimateAPI.Sources)
	router.GET("/fieldclimate/data/:sensor/:group/:from/:to", fieldClimateAPI.Data)

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
