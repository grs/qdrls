package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
        "text/tabwriter"

	"pack.ag/amqp"
)

type QueryHandler interface {
	QualifiedType() string
	AttributeNames() []interface{}
	Header(items []interface{}) string
	Record(items []interface{}) string
}

type Attribute struct {
	name string
	alias string
}

func DisplayNames(attributes []Attribute) []interface{} {
	copy := make([]interface{}, len(attributes))
	for i, v := range attributes {
		if len(v.alias) > 0 {
			copy[i] = v.alias
		} else {
			copy[i] = v.name
		}
	}
	return copy[:]
}

func DisplayName(name string, attributes []Attribute) string {
	for _, v := range attributes {
		if name == v.name {
			if len(v.alias) > 0 {
				return v.alias
			} else {
				return v.name
			}
		}
	}
	return name
}

func RealNames(attributes []Attribute) []interface{} {
	copy := make([]interface{}, len(attributes))
	for i, v := range attributes {
		copy[i] = v.name
	}
	return copy[:]
}

func DefaultLinkAttributes() []Attribute {
	return []Attribute{
		{"linkType","type"},
		{"linkDir","dir"},
		{"connectionId","conn"},
		{"identity","id"},
		{"owningAddr","addr"},
		{"capacity","cpcty"},
		{"linkName", "name"},
		{"undeliveredCount","undel"},
		{"unsettledCount","unsett"},
		{"deliveryCount","del"},
		{"acceptedCount","acc"},
		{"releasedCount","rel"},
		{"modifiedCount","mod"},
		{"rejectedCount","rej"},
		{"presettledCount","presett"},
		{"droppedPresettledCount","psdrop"}}
}

func DefaultAddressAttributes() []Attribute {
	return []Attribute{
		{"key", "addr"},
		{"distribution", "distrib"},
		{"priority", "pri"},
		{"subscriberCount", "local"},
		{"remoteCount", "remote"},
		{"deliveriesEgress", "out"},
		{"deliveriesIngress", "in"},
		{"deliveriesTransit", "thru"}}
}

func GetAttribute(name string, attributes []Attribute) Attribute {
	for _, v := range attributes {
		if v.name == name || v.alias == name {
			return v
		}
	}
	return Attribute{name:name}
}

func GetAttributes(selected string, all []Attribute, defaults []Attribute) []Attribute {
	if len(selected) > 0 {
		list := strings.Split(selected, ",")
		copy := make([]Attribute, len(list))
		for i, v := range list {
			copy[i] = GetAttribute(v, all)
		}
		return copy[:]
	} else {
		return defaults
	}
}

func (h DefaultQueryHandler) AttributeNames() []interface{} {
	return RealNames(h.attributes)
}

func (h *DefaultQueryHandler) SetSelectedAttributes(selected string) {
	h.attributes = GetAttributes(selected, h.defaultAttributes, h.defaultAttributes)
}

func (h DefaultQueryHandler) QualifiedType() string {
	return h.typename
}

func stringify(items []interface{}) []string {
	s := make([]string, len(items))
	for i := range items {
		s[i] = fmt.Sprintf("%v", items[i])
	}
	return s
}

func (h DefaultQueryHandler) Record(items []interface{}) string {
	s := stringify(items)
	return strings.Join(s, "\t")
}

func (h DefaultQueryHandler) Header(items []interface{}) string {
	s := stringify(items)
	for i, v := range s {
		s[i] = strings.ToUpper(DisplayName(v, h.attributes))
	}
	return strings.Join(s, "\t")
}

type Entity struct {
	typename string
	alias string
	defaultAttributes []Attribute
}

type DefaultQueryHandler struct {
	Entity
	attributes []Attribute
}

func GetType(typename string) Entity {
	entities := []Entity {
		{"org.apache.qpid.dispatch.router.link", "link", DefaultLinkAttributes()},
		{"org.apache.qpid.dispatch.router.address", "address", DefaultAddressAttributes()},
	}
	for _, e := range(entities) {
		if typename == e.typename || typename == e.alias {
			return e
		}
	}
	return Entity{typename:typename}
}

func getQueryHandler(typename string, attributes string) QueryHandler {
	var handler DefaultQueryHandler
	handler.Entity = GetType(typename)
	handler.SetSelectedAttributes(attributes)
	return handler
}

func authOption(username string, password string) amqp.ConnOption {
	if (len(password) > 0 && len(username) > 0) {
		return amqp.ConnSASLPlain(username, password)
	} else {
		return amqp.ConnSASLAnonymous()
	}
}

func main() {
	url := flag.String("url", "amqp://localhost:5672", "URL to connect to")
	typename := flag.String("type", "link", "Type of the entities to list")
	attributes := flag.String("attributes", "", "Comma separated list of attributes to display")
	username := flag.String("username", "", "User to connect as")
	password := flag.String("password", "", "Password to connect with")
	flag.Parse()

	client, err := amqp.Dial(*url, authOption(*username, *password), amqp.ConnMaxFrameSize(4294967295))
	if err != nil {
		log.Fatal("Failed to connect to ", *url, ": ", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Failed to create session:", err)
	}

	ctx := context.Background()

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(""),
		amqp.LinkAddressDynamic(),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Failed to create receiver:", err)
	}
	sender, err := session.NewSender(
		amqp.LinkTargetAddress("$management"),
	)
	if err != nil {
		log.Fatal("Failed to create sender:", err)
	}

	var request amqp.Message
	var properties amqp.MessageProperties
	properties.ReplyTo = receiver.Address()
	properties.CorrelationID = 1
	request.Properties = &properties
	request.ApplicationProperties = make(map[string]interface{})
	request.ApplicationProperties["operation"] = "QUERY"
	handler := getQueryHandler(*typename, *attributes)
	request.ApplicationProperties["entityType"] = handler.QualifiedType()
	var body = make(map[string]interface{})
	body["attributeNames"] = handler.AttributeNames()
	request.Value = body

	err = sender.Send(ctx, &request)
	if err != nil {
		log.Fatal("Could not send request:", err)
	}

	sender.Close(ctx)

	response, err := receiver.Receive(ctx)
	if err != nil {
		log.Fatal("Failed to receive reponse:", err)
	}
	response.Accept()

	if status, ok := response.ApplicationProperties["statusCode"].(int32); ok && status == 200 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if top, ok := response.Value.(map[string]interface{}); ok {
			fields := top["attributeNames"].([]interface{})
			fmt.Fprintln(w, handler.Header(fields))
			results := top["results"].([]interface{})
			for _, r := range results {
				o := r.([]interface{})
				fmt.Fprintln(w, handler.Record(o))
			}
			w.Flush()
		} else {
			fmt.Printf("Bad response: %s\n", response.Value)
		}
	} else {
		fmt.Printf("ERROR: %s\n", response.ApplicationProperties["statusDescription"])
	}
	receiver.Close(ctx)
}
