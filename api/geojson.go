package api

import (
	"math"
)

type FeatureCollection struct {
	Type     string     `json:"type"`
	Features *[]Feature `json:"features"`
}

type Feature struct {
	Geometry   *Polygon   `json:"geometry"`
	Properties Properties `json:"properties"`
	Type       string     `json:"type"`
}

type Properties struct {
	Id    int     `json:"id"`
	Value float32 `json:"value"`
}

type Point struct {
	Coordinates [1][2]float32 `json:"coordinates"`
	Type        string        `json:"type"`
}

type Polygon struct {
	Coordinates [1][5][2]float32 `json:"coordinates"`
	Type        string           `json:"type"`
}

func PointFromLatLng(lat, lng float32) *Point {
	return &Point{
		Coordinates: [1][2]float32{
			{lng, lat},
		},
		Type: "Point",
	}
}

func PolygonFromLatLng(lat, lng float32) *Polygon {
	latAccuracy := float32(0.004491576385357491) // 180 * 1000 / 40075017
	lngAccuracy := latAccuracy / float32(math.Cos((math.Pi/180)*float64(lat)))
	return &Polygon{[1][5][2]float32{{
		{lng - lngAccuracy, lat - latAccuracy},
		{lng - lngAccuracy, lat + latAccuracy},
		{lng + lngAccuracy, lat + latAccuracy},
		{lng + lngAccuracy, lat - latAccuracy},
		{lng - lngAccuracy, lat - latAccuracy},
	}}, "Polygon"}
}
