package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"iot_client/data"
	"iot_client/datastore"

	aosong "github.com/d2r2/go-aosong"
	i2c "github.com/d2r2/go-i2c"
	jwt "github.com/dgrijalva/jwt-go"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/jasonlvhit/gocron"
)

var (
	deviceID = flag.String("device", "", "Cloud IoT Core Device ID")
	bridge   = struct {
		host *string
		port *string
	}{
		flag.String("mqtt_host", "mqtt.googleapis.com", "MQTT Bridge Host"),
		flag.String("mqtt_port", "8883", "MQTT Bridge Port"),
	}
	projectID                                = flag.String("project", "", "GCP Project ID")
	registryID                               = flag.String("registry", "", "Cloud IoT Registry ID (short form)")
	region                                   = flag.String("region", "", "GCP Region")
	privateKey                               = flag.String("private_key", "", "Path to private key file")
	intervalUpdateMutex                      = sync.Mutex{}
	intervalSet                              = false
	updateInterval                           = uint64(10)
	responseChannel     chan responseMessage = make(chan responseMessage)
	topic                                    = Topics{}
	client              MQTT.Client
	db                  datastore.Datastore
)

type responseMessage struct {
	Topic    string
	Qos      byte
	Retained bool
	Payload  interface{}
}

type Topics struct {
	Commands  string
	Telemetry string
}

func main() {
	log.Println("[main] Entered")

	log.Println("[main] Flags")
	flag.Parse()

	log.Println("[main] Loading Google's roots")
	certpool := x509.NewCertPool()
	pemCerts, err := data.Asset("roots.pem")
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	log.Println("[main] Creating TLS Config")

	config := &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{},
		MinVersion:         tls.VersionTLS12,
	}

	topic = Topics{
		Commands:  fmt.Sprintf("/devices/%v/commands/#", *deviceID),
		Telemetry: fmt.Sprintf("/devices/%v/events", *deviceID),
	}

	clientID := fmt.Sprintf("projects/%v/locations/%v/registries/%v/devices/%v",
		*projectID,
		*region,
		*registryID,
		*deviceID,
	)

	log.Println("[main] Creating MQTT Client Options")
	log.Printf("client id: %s", clientID)
	opts := MQTT.NewClientOptions()

	broker := fmt.Sprintf("ssl://%v:%v", *bridge.host, *bridge.port)
	log.Printf("[main] Broker '%v'", broker)

	opts.AddBroker(broker)
	opts.SetClientID(clientID).SetTLSConfig(config)

	opts.SetUsername("unused")

	// Incoming
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[handler] Payload: %v\n", msg.Payload())
	})

	db, err = datastore.New()
	if err != nil {
		log.Fatal(err)
	}

	intitaliseClient(opts)
	gocron.Every(59).Minutes().Do(intitaliseClient, opts)

	setUpdateInterval(updateInterval)

	go sendMessages(responseChannel)
	<-gocron.Start()

	log.Println("[main] MQTT Client Disconnecting")
	client.Disconnect(250)

	log.Println("[main] Done")
}

func setUpdateInterval(interval uint64) {
	intervalUpdateMutex.Lock()
	defer intervalUpdateMutex.Unlock()
	if intervalSet {
		gocron.Remove(readTempAndHumidity)
	}
	gocron.Every(interval).Minutes().Do(readTempAndHumidity)
	log.Printf("[setUpdateInterval] interval set to %d", interval)
	intervalSet = true
}

func readTempAndHumidity() {
	log.Println("[readTempAndHumidity] Read data")
	i2c, err := i2c.NewI2C(0x5C, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer i2c.Close()

	sensor := aosong.NewSensor(aosong.AM2320)

	rh, t, err := sensor.ReadRelativeHumidityAndTemperature(i2c)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Relative humidity = %v\n", rh)
	log.Printf("Temperature in celsius = %v\n", t)
	hp := datastore.TelemetryValues{
		Temperature: t,
		Humidity:    rh,
		Timestamp:   time.Now().Unix(),
	}
	jsonBytes, err := json.Marshal(hp)
	if err != nil {
		log.Fatal(err)
	}

	db.SaveValues(hp)

	responseChannel <- responseMessage{
		Topic:    topic.Telemetry,
		Qos:      0,
		Retained: false,
		Payload:  string(jsonBytes),
	}
}

func sendMessages(responseChannel chan responseMessage) {
	log.Println("[sendMessages] waiting to send messages")
	for message := range responseChannel {
		log.Println("[sendMessages] Sending message")
		token := client.Publish(
			message.Topic,
			message.Qos,
			message.Retained,
			message.Payload)
		returnedBeforeTimeout := token.WaitTimeout(5 * time.Second)
		if !returnedBeforeTimeout {
			log.Println("[sendMessages] ERROR wait for publish timmed out")
		}
	}
}

func intitaliseClient(opts *MQTT.ClientOptions) {
	if client != nil {
		log.Println("[intitaliseClient] MQTT Client Disconnecting old")
		client.Disconnect(250)
	}
	tokenString := generateTokenString()

	opts.SetPassword(tokenString)
	client = MQTT.NewClient(opts)
	log.Println("[intitaliseClient] MQTT Client Connecting")
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	go subscribeForCommands(*deviceID, responseChannel)
}

func generateTokenString() string {
	token := jwt.New(jwt.SigningMethodRS256)
	token.Claims = jwt.StandardClaims{
		Audience:  *projectID,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}

	log.Println("[generateTokenString] Load Private Key")
	keyBytes, err := ioutil.ReadFile(*privateKey)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("[generateTokenString] Parse Private Key")
	key, err := jwt.ParseRSAPrivateKeyFromPEM(keyBytes)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("[generateTokenString] Sign String")
	tokenString, err := token.SignedString(key)
	if err != nil {
		log.Fatal(err)
	}
	return tokenString
}

func subscribeForCommands(deviceID string, responseChannel chan responseMessage) {
	log.Println("[subscribeForCommands] Creating Subscription")
	client.Subscribe(topic.Commands, 0, func(client MQTT.Client, msg MQTT.Message) {
		log.Printf("[handler] Topic: %v\n", msg.Topic())
		log.Printf("[handler] Payload: %v\n", string(msg.Payload()))
		log.Printf("[handler] MessageID: %d\n", msg.MessageID())
	})

}
