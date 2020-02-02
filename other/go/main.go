package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"iot_client/data"

	aosong "github.com/d2r2/go-aosong"
	i2c "github.com/d2r2/go-i2c"
	jwt "github.com/dgrijalva/jwt-go"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/jasonlvhit/gocron"
	"github.com/spf13/viper"
)

type responseMessage struct {
	Topic    string
	Qos      byte
	Retained bool
	Payload  interface{}
}

type TelemetryValues struct {
	Temperature  float32 `json:"temperature"`
	Humidity     float32 `json:"humidity"`
	Timestamp    int64   `json:"timestamp"`
	FriendlyName string  `json:"friendlyName"`
}

type Topics struct {
	Commands  string
	Telemetry string
}

type dataToSaveToDisk struct {
	TelemetryValues []TelemetryValues `json:"telemetryValues"`
}

type AppConfig struct {
	DeviceID       string `json:"deviceID"`
	ProjectID      string `json:"projectID"`
	RegistryID     string `json:"registryID"`
	FriendlyName   string `json:"friendlyName"`
	RegionID       string `json:"regionID"`
	PrivateKey     string `json:"privateKey"`
	Host           string `json:"host"`
	Port           string `json:"port"`
	OfflineData    string `json:"offlineData"`
	UpdateInterval uint64 `json:"updateInterval"`
}

var (
	appConfig                     = AppConfig{}
	intervalUpdateMutex           = sync.Mutex{}
	intervalSet                   = false
	telemetryDataToSend chan bool = make(chan bool)
	topic                         = Topics{}
	telemetryMutex                = sync.Mutex{}
	telemetry                     = []TelemetryValues{}
	osSignals                     = make(chan os.Signal, 1)
	client              MQTT.Client
)

func main() {
	log.Println("[main] Entered")

	log.Println("[main] Flags")
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath("/etc/iot_client/")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("[main] Error loading config, %v", err)
	}
	err = viper.Unmarshal(&appConfig)
	if err != nil {
		log.Fatalf("[main] viper is unable to decode into struct, %v", err)
	}

	telemetryFromDisk, err := Load()
	if err != nil {
		log.Println("[main] Error loading data")
	} else {
		telemetry = telemetryFromDisk
	}

	signal.Notify(osSignals, os.Interrupt)
	go saveTelemetryArrayToDisk()

	log.Println("[main] Loading Google's roots")
	certpool := x509.NewCertPool()
	pemCerts, err := data.Asset("roots.pem")
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}

	log.Println("[main] Creating TLS Config")

	tlsConfig := &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{},
		MinVersion:         tls.VersionTLS12,
	}

	topic = Topics{
		Commands:  fmt.Sprintf("/devices/%s/commands/#", appConfig.DeviceID),
		Telemetry: fmt.Sprintf("/devices/%s/events", appConfig.DeviceID),
	}

	clientID := fmt.Sprintf("projects/%s/locations/%s/registries/%s/devices/%s",
		appConfig.ProjectID,
		appConfig.RegionID,
		appConfig.RegistryID,
		appConfig.DeviceID,
	)

	log.Println("[main] Creating MQTT Client Options")
	log.Printf("client id: %s", clientID)
	opts := MQTT.NewClientOptions()

	broker := fmt.Sprintf("ssl://%s:%s", appConfig.Host, appConfig.Port)
	log.Printf("[main] Broker '%s'", broker)

	opts.AddBroker(broker)
	opts.SetClientID(clientID).SetTLSConfig(tlsConfig)

	opts.SetUsername("unused")

	// Incoming
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("[handler] Topic: %v\n", msg.Topic())
		fmt.Printf("[handler] Payload: %v\n", msg.Payload())
	})

	intitaliseClient(opts)
	gocron.Every(29).Minutes().Do(intitaliseClient, opts)

	go sendMessages(telemetryDataToSend)

	setUpdateInterval(appConfig.UpdateInterval)

	<-gocron.Start()

	log.Println("[main] MQTT Client Disconnecting")
	client.Disconnect(250)

	log.Println("[main] Done")
}

func setUpdateInterval(interval uint64) {
	defer intervalUpdateMutex.Unlock()
	if intervalSet {
		gocron.Remove(readTempAndHumidity)
	}

	currentTime := time.Now()
	startTime := currentTime.Truncate(time.Minute).Add(time.Minute * time.Duration(int(interval)-(currentTime.Minute()%int(interval))))
	gocron.Every(interval).Minutes().From(&startTime).Do(readTempAndHumidity)
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

	tv := TelemetryValues{
		Temperature:  t,
		Humidity:     rh,
		Timestamp:    time.Now().Unix(),
		FriendlyName: appConfig.FriendlyName,
	}
	addToTelemetryArray(tv)
}

func addToTelemetryArray(tv TelemetryValues) {
	log.Printf("[addToTelemetryArray]")
	telemetryMutex.Lock()
	defer telemetryMutex.Unlock()
	telemetry = append(telemetry, tv)
	telemetryDataToSend <- true
}

func saveTelemetryArrayToDisk() {
	oscall := <-osSignals
	log.Printf("[saveTelemetryArrayToDisk] system call:%+v", oscall)
	dtstd := dataToSaveToDisk{
		TelemetryValues: telemetry,
	}

	f, err := os.Create(fmt.Sprintf("%s/iot-offline-data.json", appConfig.OfflineData))
	if err != nil {
		log.Println("unable to create save file, ", err)
	}
	defer f.Close()
	r, err := json.Marshal(dtstd)
	if err != nil {
		log.Println("unable to marshal save data, ", err)
	}
	_, err = io.Copy(f, bytes.NewReader(r))
	if err != nil {
		log.Println("unable to save data, ", err)
	}
}

func Load() ([]TelemetryValues, error) {
	output := []TelemetryValues{}
	data := dataToSaveToDisk{}
	f, err := os.Open(fmt.Sprintf("%s/iot-offline-data.json", appConfig.OfflineData))
	if err != nil {
		return output, err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(data)
	return data.TelemetryValues, err
}

func connected() (ok bool) {
	log.Println("[connected]")
	_, err := http.Get("https://google.com/")
	if err != nil {
		return false
	}
	return true
}

func sendMessages(responseChannel chan bool) {
	log.Println("[sendMessages] waiting to send messages")
	for _ = range telemetryDataToSend {
		log.Println("[sendMessages] Sending messages")
		telemetryMutex.Lock()
		deadLetterTelemetry := []TelemetryValues{}
		if connected() {
			log.Println("[sendMessage] connected to the internet")
			for _, telemetryData := range telemetry {
				jsonBytes, err := json.Marshal(telemetryData)
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("[sendMessages] payload: %s", string(jsonBytes))
				message := responseMessage{
					Topic:    topic.Telemetry,
					Qos:      0,
					Retained: false,
					Payload:  string(jsonBytes),
				}
				token := client.Publish(
					message.Topic,
					message.Qos,
					message.Retained,
					message.Payload)
				returnedBeforeTimeout := token.WaitTimeout(5 * time.Second)
				if !returnedBeforeTimeout {
					log.Println("[sendMessages] ERROR wait for publish timmed out")
					deadLetterTelemetry = append(deadLetterTelemetry, telemetryData)
				}
			}
		} else {
			log.Println("[sendMessages] not connected to internet")
			deadLetterTelemetry = telemetry
		}

		if len(deadLetterTelemetry) > 0 {
			log.Printf("[sendMessages] dead letters: %v", deadLetterTelemetry)
		}

		telemetry = deadLetterTelemetry
		telemetryMutex.Unlock()
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
	go subscribeForCommands(appConfig.DeviceID)
}

func generateTokenString() string {
	token := jwt.New(jwt.SigningMethodRS256)
	token.Claims = jwt.StandardClaims{
		Audience:  appConfig.ProjectID,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}

	log.Println("[generateTokenString] Load Private Key")
	keyBytes, err := ioutil.ReadFile(appConfig.PrivateKey)
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

func subscribeForCommands(deviceID string) {
	log.Println("[subscribeForCommands] Creating Subscription")
	client.Subscribe(topic.Commands, 0, func(client MQTT.Client, msg MQTT.Message) {
		log.Printf("[handler] Topic: %v\n", msg.Topic())
		log.Printf("[handler] Payload: %v\n", string(msg.Payload()))
		log.Printf("[handler] MessageID: %d\n", msg.MessageID())
	})

}
