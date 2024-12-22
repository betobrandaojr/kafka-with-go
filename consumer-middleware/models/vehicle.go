package models

type Vehicle struct {
	ID     int    `json:"id"`
	Plate  string `json:"plate"`
	Brand  string `json:"brand"`
	Modelo string `json:"modelo"`
}
