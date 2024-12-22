package models

type Appointment struct {
	ID          int    `json:"id"`
	VehicleID   int    `json:"vehicle_id"`
	Lat         string `json:"lat"`
	Long        string `json:"long"`
	ProcessDate string `json:"process_date"`
}
