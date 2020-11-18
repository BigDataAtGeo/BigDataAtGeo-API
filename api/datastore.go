package api

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
)

type Variable struct {
	Id          string  `json:"var_id"`
	Var         string  `json:"var"`
	Unit        string  `json:"unit"`
	Description string  `json:"description"`
	ColorMap    string  `json:"colormap"`
	Min         float32 `json:"min"`
	Max         float32 `json:"max"`
}

type Coordinate struct {
	Lat float32
	Lng float32
}

type DataStore struct {
	Rows             []Row
	Variables        []Variable
	// use uint8 to save memory,
	ById             map[int]map[uint8]map[uint8][]*Row
	ByTimeRange      map[uint8]map[uint8]map[uint8][]*Row
	// this requires some mapping ...
	Variable2Id      map[string]uint8
	Id2Variable      map[uint8]string
	Scenario2Id      map[string]uint8
	Id2Scenario      map[uint8]string
	TimeRange2Id     map[string]uint8
	Id2TimeRange     map[uint8]string
	//
	Coordinates      map[int]Coordinate
	SortedScenarios  []string
	SortedTimeRanges []string
}

type Row struct {
	Id        int     `json:"id"`
	Lat       float32 `json:"lat"`
	Lon       float32 `json:"lon"`
	Value     float32 `json:"value"`
	TimeRange uint8   `json:"timerange"`
	Variable  uint8   `json:"var"`
	Scenario  uint8   `json:"scenario"`
}

func NewDataStore(capacity int) DataStore {
	return DataStore{
		Rows:             make([]Row, 0, capacity),
		ById:             make(map[int]map[uint8]map[uint8][]*Row),
		ByTimeRange:      make(map[uint8]map[uint8]map[uint8][]*Row),
		Variable2Id:      make(map[string]uint8),
		Id2Variable:      make(map[uint8]string),
		Scenario2Id:      make(map[string]uint8),
		Id2Scenario:      make(map[uint8]string),
		TimeRange2Id:     make(map[string]uint8),
		Id2TimeRange:     make(map[uint8]string),
		Coordinates:      make(map[int]Coordinate),
		Variables:        make([]Variable, 0),
		SortedScenarios:  make([]string, 0),
		SortedTimeRanges: make([]string, 0),
	}
}

func (dataStore *DataStore) addTimeRange(timeRange string) {
	if _, isPresent := dataStore.TimeRange2Id[timeRange]; !isPresent {
		if len(dataStore.TimeRange2Id) == 255 {
			panic(errors.New("maximum amount of time ranges reached (uint8)"))
		}
		id := uint8(len(dataStore.TimeRange2Id))
		dataStore.TimeRange2Id[timeRange] = id
		dataStore.Id2TimeRange[id] = timeRange
	}
}

func (dataStore *DataStore) addVariable(variable string) {
	if _, isPresent := dataStore.Variable2Id[variable]; !isPresent {
		if len(dataStore.Variable2Id) == 255 {
			panic(errors.New("maximum amount of variables reached (uint8)"))
		}
		id := uint8(len(dataStore.Variable2Id))
		dataStore.Variable2Id[variable] = id
		dataStore.Id2Variable[id] = variable
	}
}

func (dataStore *DataStore) addScenario(scenario string) {
	if _, isPresent := dataStore.Scenario2Id[scenario]; !isPresent {
		if len(dataStore.Scenario2Id) == 255 {
			panic(errors.New("maximum amount of scenarios reached (uint8)"))
		}
		id := uint8(len(dataStore.Scenario2Id))
		dataStore.Scenario2Id[scenario] = id
		dataStore.Id2Scenario[id] = scenario
	}
}

func (dataStore *DataStore) addCoordinates(id int, lat, lng float32) {
	dataStore.Coordinates[id] = Coordinate{lat, lng}
}

func (dataStore *DataStore) addRow(id int, lat, lng, value float32, timeRange, variable, scenario string) {
	dataStore.addTimeRange(timeRange)
	dataStore.addScenario(scenario)
	dataStore.addVariable(variable)
	dataStore.addCoordinates(id, lat, lng)
	timeRangeId := dataStore.TimeRange2Id[timeRange]
	variableId := dataStore.Variable2Id[variable]
	scenarioId := dataStore.Scenario2Id[scenario]
	row := Row{
		id,
		lat,
		lng,
		value,
		timeRangeId,
		variableId,
		scenarioId,
	}
	if _, isPresent := dataStore.ById[id]; !isPresent {
		dataStore.ById[id] = make(map[uint8]map[uint8][]*Row)
	}
	if _, isPresent := dataStore.ById[id][variableId]; !isPresent {
		dataStore.ById[id][variableId] = make(map[uint8][]*Row)
	}
	if _, isPresent := dataStore.ByTimeRange[timeRangeId]; !isPresent {
		dataStore.ByTimeRange[timeRangeId] = make(map[uint8]map[uint8][]*Row)
	}
	if _, isPresent := dataStore.ByTimeRange[timeRangeId][variableId]; !isPresent {
		dataStore.ByTimeRange[timeRangeId][variableId] = make(map[uint8][]*Row)
	}
	dataStore.ById[id][variableId][scenarioId] =
		append(dataStore.ById[id][variableId][scenarioId], &row)
	dataStore.ByTimeRange[timeRangeId][variableId][scenarioId] =
		append(dataStore.ByTimeRange[timeRangeId][variableId][scenarioId], &row)
	dataStore.Rows = append(dataStore.Rows, row)
}

func loadVariables(filePath string) []Variable {
	jsonFile, err := ioutil.ReadFile(filePath)
	var variables []Variable
	err = json.Unmarshal(jsonFile, &variables)
	if err != nil {
		fmt.Println(err.Error())
	}
	return variables
}

func countLines(filePath string) (int, error) {
	reader, err := os.Open(filePath)
	if err != nil {
		log.Fatal("could not open file", filePath)
	}
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := reader.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func loadDataStore(filePath string) DataStore {
	amountLines, _ := countLines(filePath)
	dataFrame := NewDataStore(amountLines)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("could not open file '", filePath, "'")
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 7
	reader.ReuseRecord = true
	reader.Read()

	for {
		parts, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		id, _ := strconv.Atoi(parts[0])
		lat, _ := strconv.ParseFloat(parts[1], 32)
		lon, _ := strconv.ParseFloat(parts[2], 32)
		value, _ := strconv.ParseFloat(parts[4], 32)
		timeRange := parts[3]
		variable := parts[5]
		scenario := parts[6]
		dataFrame.addRow(id, float32(lat), float32(lon), float32(value), timeRange, variable, scenario)
	}

	return dataFrame
}

func LoadDataStore(dataDirectory string) DataStore {
	dataStore := loadDataStore(path.Join(dataDirectory, "data.csv"))
	dataStore.Variables = loadVariables(path.Join(dataDirectory, "variables.json"))

	for _, timeRange := range dataStore.Id2TimeRange {
		dataStore.SortedTimeRanges = append(dataStore.SortedTimeRanges, timeRange)
	}
	sort.Strings(dataStore.SortedTimeRanges)

	for _, scenario := range dataStore.Id2Scenario {
		dataStore.SortedScenarios = append(dataStore.SortedScenarios, scenario)
	}
	sort.Strings(dataStore.SortedScenarios)

	return dataStore
}
