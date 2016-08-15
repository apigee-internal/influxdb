package gossip

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

// CreateShardData foo
type CreateShardData struct {
	database        string
	retentionPolicy string
	shardID         uint64
	enabled         bool
}

// Route struct used to store Route params
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

// Routes slice
type Routes []Route

// StartRouter starts a listener for create/write shards
func (s *TSDBStore) StartRouter() {
	router := s.NewRouter()
	c := cors.New(cors.Options{
		AllowedMethods: []string{"POST"},
	})
	handler := c.Handler(router)
	log.Fatal(http.ListenAndServe(":8888", handler))
}

// NewRouter handles the http routes for Clusterflux
func (s *TSDBStore) NewRouter() *mux.Router {

	var routes = Routes{
		Route{
			"Create",
			"POST",
			"/create",
			s.CreateShardLocal,
		},
		Route{
			"Write",
			"POST",
			"/write",
			s.WriteToShardLocal,
		},
		Route{
			"Read",
			"GET",
			"/read/shards",
			s.ReadShardsToRemote,
		},
	}

	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}
	return router
}
