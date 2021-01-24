package api

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
)

type FieldClimateAPI struct {
	signer     HmacSigner
	httpClient http.Client
	url        string
}

func NewFieldClimateAPI(publicKey, privateKey, apiUrl string) FieldClimateAPI {
	return FieldClimateAPI{
		signer:  NewHmacSigner(publicKey, privateKey, sha256.New),
		httpClient: http.Client{},
		url: apiUrl,
	}
}

type source struct {
	Name struct {
		Custom   string `json:"custom"`
		Original string `json:"original"`
	} `json:"name"`

	Position struct {
		Altitude float32 `json:"altitude"`
		Geo      struct {
			Coordinates [2]float32 `json:"coordinates"`
		} `json:"geo"`
	} `json:"position"`

	Dates struct {
		Created           string `json:"created_at"`
		LastCommunication string `json:"last_communication"`
		MaxDate           string `json:"max_date"`
		MinDate           string `json:"min_date"`
	} `json:"dates"`
}

func (api *FieldClimateAPI) Sources(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	request, _ := http.NewRequest("GET", api.url + "/user/stations", nil)
	api.signer.Sign(&request.Header, []byte("GET"), []byte("/user/stations"))
	resp, _ := api.httpClient.Do(request)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	var response []source
	err := json.Unmarshal(body, &response)
	if err != nil {
		http.Error(w, "fieldclimate API not reachable", http.StatusNotFound)
		return
	}

	writeHeader(w)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (api *FieldClimateAPI) Data(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
    sensor, group := ps.ByName("sensor"), ps.ByName("group")
	from, to := ps.ByName("from"), ps.ByName("to")

	if sensor == "" || group == "" || from == "" || to == "" {
		http.Error(w, "no data found for arguments", http.StatusNoContent)
		return
	}

    endPoint := "/data/optimized/" + sensor + "/" + group + "/from/" + from + "/to/" + to
    request, _ := http.NewRequest("GET", api.url + endPoint, nil)
	api.signer.Sign(&request.Header, []byte("GET"), []byte(endPoint))
	resp, _ := api.httpClient.Do(request)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body);
	if err != nil {
		http.Error(w, "", resp.StatusCode)
		return
	} else if len(body) == 0 {
		http.Error(w, "", http.StatusNoContent)
		return
	}

	writeHeader(w)
	w.Write(body)
}

