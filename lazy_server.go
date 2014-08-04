package main

import (
	"encoding/json"
	"fmt"
	graceful "github.com/braintree/manners"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	jsonschema "github.com/xeipuuv/gojsonschema"
	"io"
	"labix.org/v2/mgo"
	"path/filepath"
	// golog "log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	logger        log.Logger
	session       *mgo.Session
	eventsSchema  *jsonschema.JsonSchemaDocument
	eventsChannel chan []interface{}
)

const (
	MgoDatabaseHost         = "localhost"
	MgoDatabaseName         = "analytics_development"
	MgoEventsCollectionName = "events"
)

func mgoConnect() {
	// mgo.SetLogger(golog.New(os.Stdout,
	// 	"MONGO: ",
	// 	golog.Ldate|golog.Ltime|golog.Lshortfile))
	// mgo.SetDebug(true)
	// mgo.SetStats(true)

	var err error
	session, err = mgo.Dial(MgoDatabaseHost)
	if err != nil {
		panic(err)
	}

	safe := session.Safe()
	logger.Info("MongoDB Safe info", "W", safe.W, "WMode", safe.WMode, "WTimeout", safe.WTimeout, "J", safe.J, "FSync", safe.FSync)
}

// http://denis.papathanasiou.org/2012/10/14/go-golang-and-mongodb-using-mgo/
// http://www.goinggo.net/2014/02/running-queries-concurrently-against.html
func getSession() *mgo.Session {
	if session == nil {
		mgoConnect()
	}
	return session.Copy()
}

func getSessionWithCollection(collection string) (*mgo.Session, *mgo.Collection) {
	s := getSession()
	c := s.DB(MgoDatabaseName).C(collection)
	return s, c
}

type Runtime struct{}

func NewRuntime() *Runtime {
	return &Runtime{}
}

func (this *Runtime) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	start := time.Now()
	next(rw, r)
	requestTime := float64(time.Since(start))

	//if rw.Header().Get("X-Runtime") == "" {
	requestTimeSeconds := requestTime / float64(time.Second)
	rw.Header().Add("X-Runtime", fmt.Sprintf("%0.6f", requestTimeSeconds))

	logger.Debug("Request has finished in", "seconds", requestTimeSeconds)
	//}
}

type HealthzHandler struct{}

func (handler *HealthzHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, `{"status" : "ok"}`)
}

func HelloWorldHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "hello world!")
}

func ValidateStringField(i map[string]interface{}, fieldName string) error {
	var field interface{}
	var ok bool
	if field, ok = i[fieldName]; !ok {
		return fmt.Errorf("%s is not present", fieldName)
	}

	if stringField, ok := field.(string); !ok {
		return fmt.Errorf("%s is not a string", fieldName)
	} else {
		if stringField == "" {
			return fmt.Errorf("%s is empty", fieldName)
		}
	}

	return nil
}

func ValidateIntField(i map[string]interface{}, fieldName string) error {
	var field interface{}
	var ok bool
	if field, ok = i[fieldName]; !ok {
		return fmt.Errorf("%s is not present", fieldName)
	}

	if _, ok := field.(float64); !ok {
		return fmt.Errorf("%s is not a number", fieldName)
	}

	return nil
}

func ValidateEvent(event map[string]interface{}) error {
	// event, ok := ievent.(map[string]interface{})
	// if !ok {
	// 	return errors.New("is not an event structure")
	// }

	var err error

	if err = ValidateStringField(event, "type"); err != nil {
		return err
	}
	if err = ValidateIntField(event, "distinct_id"); err != nil {
		return err
	}
	if err = ValidateStringField(event, "session_id"); err != nil {
		return err
	}
	return nil
}

func EventsHandler(w http.ResponseWriter, r *http.Request) {
	// validate content type

	// read the body and decode json payload
	decoder := json.NewDecoder(r.Body)
	var v interface{}
	err := decoder.Decode(&v)

	// validate if json payload is properly formed
	if err != nil {
		logger.Error(err.Error())
		panic("invalid json payload")
	}

	// validate if json payload contains event or events
	eventsPayload := v.(map[string]interface{})

	result := eventsSchema.Validate(eventsPayload)
	if !result.Valid() {
		for _, desc := range result.Errors() {
			logger.Error(desc.String())
		}
	}

	// validate event
	if events, ok := eventsPayload["events"]; ok {
		// convert timestamp to time
		for _, ievent := range events.([]interface{}) {
			event := ievent.(map[string]interface{})

			// validate basic fields
			// ValidateEvent(event)

			// check if timestamp exist first
			if timestamp, ok := event["timestamp"]; ok {
				t, err := time.Parse(time.RFC3339Nano, timestamp.(string))
				if err != nil {
					// invalid timestamp
				}
				event["timestamp"] = t
			} else {
				event["timestamp"] = time.Now()
			}
		}

		// insert events into the mongodb instance
		select {
		case eventsChannel <- events.([]interface{}):
		case <-time.After(1 * time.Second):
			logger.Error("Failed to respond below one second. Events channell is occupied.")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// return 204
		w.WriteHeader(http.StatusNoContent)
	} else {
		// events doesn't exist
	}
}

func SurveyHandler(w http.ResponseWriter, r *http.Request) {

}

func GetEventsSchemaPath() string {
	wd, _ := os.Getwd()
	return filepath.Join("file://", wd, "events_schema.json")
}

func main() {
	logger = log.New("module", "app/server")

	runtime.GOMAXPROCS(runtime.NumCPU())
	logger.Debug(fmt.Sprintf("using GOMAXPROCS value of %d", runtime.GOMAXPROCS(0)))

	mgoConnect()
	defer session.Close()

	//session.SetMode(mgo.Eventual, false)
	eventsCollection := session.DB("analytics_development").C("events")

	// event := map[string]interface{}{
	// 	"tags":        []string{"gameplay"},
	// 	"timestamp":   time.Now(),
	// 	"distinct_id": 100,
	// 	"session_id":  "a65cf71d0869c1c168717c5a8e9d2d2b",
	// 	"data": map[string]interface{}{
	// 		"version": 1,
	// 		"item":    "ammo",
	// 		"value":   15,
	// 		"area":    "level_1",
	// 		"loc": map[string]interface{}{
	// 			"type":        "Point",
	// 			"coordinates": []int{10, 11},
	// 		},
	// 	},
	// }

	events := []interface{}{
		map[string]interface{}{
			"type": "first",
		},
		map[string]interface{}{
			"type": "second",
		},
	}
	eventsCollection.Insert(events...)

	var err error
	eventsSchema, err = jsonschema.NewJsonSchemaDocument(GetEventsSchemaPath())
	if err != nil {
		logger.Error(err.Error())
		panic("Could not load events json schema")
	}

	router := mux.NewRouter()
	router.HandleFunc("/v1/track", EventsHandler).Methods("POST")
	router.Handle("/helahtz", &HealthzHandler{}).Methods("GET")
	router.HandleFunc("/", HelloWorldHandler).Methods("GET")

	stack := negroni.New()
	stack.Use(NewRuntime())
	stack.Use(negroni.NewRecovery())
	stack.Use(negroni.NewLogger())
	stack.UseHandler(router)

	server := graceful.NewServer()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGQUIT)

	go func() {
		s := <-sig
		logger.Debug("Got signal", log.Ctx{"Signal": s})
		server.Shutdown <- true
		return
	}()

	go func() {
		for {
			mstats := new(runtime.MemStats)
			runtime.ReadMemStats(mstats)
			logger.Debug("memory allocated", "kilobytes", mstats.Alloc/1024)
			time.Sleep(10 * time.Second) // or runtime.Gosched() or similar per @misterbee
		}
	}()

	eventsChannel = make(chan []interface{}, 100)

	poolSize := 10
	numWorkers := runtime.GOMAXPROCS(0) * poolSize

	for i := 0; i < numWorkers; i++ {
		go func() {
			// insert events into the mongodb instance
			mgoSession, eventsCollection := getSessionWithCollection(MgoEventsCollectionName)
			defer mgoSession.Close()

			for {
				events := <-eventsChannel
				eventsCollection.Insert(events...)
			}
		}()
	}

	logger.Info("Server is listening on port 3000")
	logger.Info("Press Ctrl+C to stop")

	server.ListenAndServe(":3000", stack)

	logger.Info("Server is shutting down")
}
