package repositories

import (
	"consumer-middleware/models"
	"database/sql"
)

func InsertVehiclePostgres(db *sql.DB, v models.Vehicle) error {
	query := `INSERT INTO _machines.vehicles (id, plate, brand, modelo) 
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (id) DO NOTHING;`
	_, err := db.Exec(query, v.ID, v.Plate, v.Brand, v.Modelo)
	return err
}

func InsertAppointmentPostgres(db *sql.DB, a models.Appointment) error {
	query := `INSERT INTO _messages.appointments (id, vehicle_id, lat, long, process_date)
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT (id) DO NOTHING;`
	_, err := db.Exec(query, a.ID, a.VehicleID, a.Lat, a.Long, a.ProcessDate)
	return err
}
