package api

import (
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
)

type indexResponse struct {
	Variables  *[]Variable `json:"variables"`
	Scenarios  *[]string   `json:"scenarios"`
	TimeRanges *[]string   `json:"timeranges"`
}

func (dataStore *DataStore) Index(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	response := indexResponse{
		Variables:  &dataStore.Variables,
		Scenarios:  &dataStore.SortedScenarios,
		TimeRanges: &dataStore.SortedTimeRanges,
	}

	writeHeader(w)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (dataStore *DataStore) AllLocationsGrid(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	timeRange, timeRangeValid := dataStore.TimeRange2Id[ps.ByName("timerange")]
	variable, varValid := dataStore.Variable2Id[ps.ByName("var")]
	scenario, scenarioValid := dataStore.Scenario2Id[ps.ByName("scenario")]

	if !(varValid && scenarioValid && timeRangeValid) {
		http.Error(w, "No data found for arguments", http.StatusNotFound)
		return
	}

	data := dataStore.ByTimeRange[timeRange][variable][scenario]
	features := make([]Feature, len(data))
	for i, row := range data {
		features[i] = Feature{
			Geometry: PolygonFromLatLng((*row).Lat, (*row).Lon),
			Properties: Properties{
				Id:    (*row).Id,
				Value: (*row).Value,
			},
			Type: "Feature",
		}
	}

	featureCollection := FeatureCollection{
		Type:     "FeatureCollection",
		Features: &features,
	}

	writeHeader(w)
	if err := json.NewEncoder(w).Encode(featureCollection); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (dataStore *DataStore) AllLocationsValues(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	timeRange, timeRangeValid := dataStore.TimeRange2Id[ps.ByName("timerange")]
	variable, varValid := dataStore.Variable2Id[ps.ByName("var")]
	scenario, scenarioValid := dataStore.Scenario2Id[ps.ByName("scenario")]

	if !(varValid && scenarioValid && timeRangeValid) {
		http.Error(w, "No data found for arguments", http.StatusNotFound)
		return
	}

	response := make(map[int]float32)

	for _, row := range dataStore.ByTimeRange[timeRange][variable][scenario] {
		response[(*row).Id] = (*row).Value
	}

	writeHeader(w)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type allTimesResponse struct {
	Lat  float32     `json:"lat"`
	Lon  float32     `json:"lon"`
	Data allTimeData `json:"data"`
}
type allTimeData struct {
	Keys   []string  `json:"keys"`
	Values []float32 `json:"values"`
}

func (dataStore *DataStore) AllTimes(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	variable, varValid := dataStore.Variable2Id[ps.ByName("var")]
	scenario, scenarioValid := dataStore.Scenario2Id[ps.ByName("scenario")]
	cellId, _ := strconv.Atoi(ps.ByName("cell_id"))
	_, cellIdValid := dataStore.Coordinates[cellId]

	if !(varValid && scenarioValid && cellIdValid) {
		http.Error(w, "No data found for arguments", http.StatusNotFound)
		return
	}

	data := dataStore.ById[cellId][variable][scenario]
	keys := make([]string, len(data))
	values := make([]float32, len(data))
	for i, row := range data {
		keys[i] = dataStore.Id2TimeRange[(*row).TimeRange]
		values[i] = (*row).Value
	}

	response := allTimesResponse{
		Lat: dataStore.Coordinates[cellId].Lat,
		Lon: dataStore.Coordinates[cellId].Lng,
		Data: allTimeData{
			Keys:   keys,
			Values: values,
		},
	}
    
    writeHeader(w)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeHeader(w http.ResponseWriter) {
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", "*")
	header.Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
