//
// Copyright (c) 2017
// Cavium
// Mainflux
// IOTech
//
// SPDX-License-Identifier: Apache-2.0
//

package distro

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	contract "github.com/edgexfoundry/go-mod-core-contracts/models"
)

type mqttSender struct {
	client MQTT.Client
	topic  string
}

// newMqttSender - create new mqtt sender
func newMqttSender(addr contract.Addressable, cert string, key string) sender {
	protocol := strings.ToLower(addr.Protocol)

	opts := MQTT.NewClientOptions()
	broker := protocol + "://" + addr.Address + ":" + strconv.Itoa(addr.Port) + addr.Path
	opts.AddBroker(broker)
	opts.SetClientID(addr.Publisher)
	opts.SetUsername(addr.User)
	opts.SetPassword(addr.Password)
	opts.SetAutoReconnect(false)

	if protocol == "tcps" || protocol == "ssl" || protocol == "tls" {
		c, err := tls.LoadX509KeyPair(cert, key)

		if err != nil {
            LoggingClient.Info(fmt.Sprintf("cert=%s, key=%s", cert, key))
			LoggingClient.Error(fmt.Sprintf("Failed loading x509 data: %s", err))
			return nil
		}

		tlsConfig := &tls.Config{
			ClientCAs:          nil,
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{c},
		}

		opts.SetTLSConfig(tlsConfig)

	}

	sender := &mqttSender{
		client: MQTT.NewClient(opts),
		topic:  addr.Topic,
	}

	return sender
}

func (sender *mqttSender) Send(data []byte, ctx context.Context) bool {
	if !sender.client.IsConnected() {
		LoggingClient.Info("Connecting to mqtt server")

		if token := sender.client.Connect(); token.Wait() && token.Error() != nil {
			LoggingClient.Error(fmt.Sprintf("Could not connect to mqtt server, drop event. Error: %s", token.Error().Error()))
			return false
		} else {
            LoggingClient.Info("Connected to mqtt server")
        }
	}

    LoggingClient.Debug(fmt.Sprintf("Sending data: %s", data))
	token := sender.client.Publish(sender.topic, 0, false, data)
	// FIXME: could be removed? set of tokens?
	token.Wait()
	if token.Error() != nil {
		LoggingClient.Error(fmt.Sprintf("Failed to send data: %s", token.Error().Error()))
		return false
	} else {
		LoggingClient.Debug(fmt.Sprintf("Data sent: %X", data))
		return true
	}
}
